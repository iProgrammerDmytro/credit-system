import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.db import IntegrityError, connection, transaction
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from ..enums import TxStatus, TxType
from ..exceptions import InsufficientCredits
from ..models import ApiKey, CreditTransaction, TxStatus, TxType, Wallet
from ..services import (
    ReserveCreditsService,
    commit_reservation,
    reverse_reservation,
    sweep_stale_reservations,
    top_up,
)


class TopUpServiceTests(TestCase):
    """
    Unit tests for functional correctness, validation, and transactional rollback.
    """

    def setUp(self):
        self.wallet = Wallet.objects.create(name="Test Wallet")

    def test_top_up_success(self):
        amount = 100
        note = "promo bonus"

        tx = top_up(self.wallet, amount, note=note)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, amount)

        self.assertIsInstance(tx, CreditTransaction)
        self.assertEqual(tx.wallet_id, self.wallet.id)
        self.assertEqual(tx.delta, amount)
        self.assertEqual(tx.tx_type, TxType.CREDIT)
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)
        self.assertEqual(tx.note, note)

    def test_raises_on_zero_or_negative_amount(self):
        # Zero
        with self.assertRaises(ValueError):
            top_up(self.wallet, 0)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 0)
        self.assertFalse(CreditTransaction.objects.filter(wallet=self.wallet).exists())

        # Negative
        with self.assertRaises(ValueError):
            top_up(self.wallet, -10)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 0)
        self.assertFalse(CreditTransaction.objects.filter(wallet=self.wallet).exists())

    def test_atomicity_rolls_back_if_tx_creation_fails(self):
        patch_path = "credits.services.top_up.CreditTransaction.objects.create"
        with patch(patch_path, side_effect=IntegrityError("forced failure")):
            with self.assertRaises(IntegrityError):
                top_up(self.wallet, 100)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 0)
        self.assertFalse(CreditTransaction.objects.filter(wallet=self.wallet).exists())


class TopUpConcurrencyTests(TransactionTestCase):
    """
    Concurrency test using TransactionTestCase so threads get real commits.
    Verifies F() update is safe under contention and totals are correct.
    """

    def setUp(self):
        self.wallet = Wallet.objects.create(name="Test Wallet")

    def _worker(self, amount: int, note: str = "threaded"):
        # Each call should be fully atomic
        try:
            tx = top_up(self.wallet, amount, note=note)
            return tx.delta
        finally:
            connection.close()

    def test_many_concurrent_top_ups(self):
        amounts = [1] * 200  # keeps reasonable to avoid slow tests;

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(self._worker, amount) for amount in amounts]
            successes, failures = 0, 0
            total_delta = 0

            for f in as_completed(futures):
                try:
                    total_delta += f.result()
                    successes += 1
                except Exception:
                    failures += 1

        self.assertEqual(failures, 0)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, total_delta)
        self.assertEqual(
            CreditTransaction.objects.filter(wallet_id=self.wallet.id).count(),
            successes,
        )
        self.assertTrue(
            CreditTransaction.objects.filter(wallet_id=self.wallet.id, delta=1).exists()
        )

    def test_invalid_amounts_do_not_affect_balance_or_create_rows(self):
        amounts = [5, 0, -3, 10, 0, 7, -1, 8]  # mix valid/invalid
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [
                (
                    pool.submit(self._worker, amount)
                    if amount > 0
                    else pool.submit(top_up, self.wallet, amount)
                )
                for amount in amounts
            ]

            successes, failures = 0, 0
            total_delta = 0

            for f in as_completed(futures):
                try:
                    res = f.result()
                    # res is a delta for _worker, None for invalid Calls
                    if isinstance(res, int):
                        total_delta += res
                        successes += 1
                except ValueError:
                    failures += 1

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, total_delta)
        self.assertEqual(
            CreditTransaction.objects.filter(wallet_id=self.wallet.id).count(),
            successes,
        )

        # Confirm that failures correspond to invalid amounts
        self.assertEqual(failures, sum(1 for a in amounts if a <= 0))


class ReverseCreditsTests(TestCase):
    def setUp(self):
        self.initial_balance = 10
        self.wallet = Wallet.objects.create(name="W", balance=self.initial_balance)
        self.reserve_credits = ReserveCreditsService()

    def test_success_decrements_balance_and_creates_pending_tx(self):
        amount = 3
        request_id = "request-1"
        note = "api"
        tx = self.reserve_credits(
            self.wallet, amount=amount, request_id=request_id, note=note
        )

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - amount)

        self.assertEqual(tx.wallet_id, self.wallet.id)
        self.assertEqual(tx.delta, -amount)
        self.assertEqual(tx.tx_type, TxType.DEBIT)
        self.assertEqual(tx.tx_status, TxStatus.PENDING)
        self.assertEqual(tx.request_id, request_id)
        self.assertEqual(tx.note, note)

    def test_zero_or_negative_amount_raises_and_no_side_effects(self):
        for bad in (0, -1, -10):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.reserve_credits(self.wallet, amount=bad)

                self.wallet.refresh_from_db()
                self.assertEqual(self.wallet.balance, self.initial_balance)
                self.assertFalse(
                    CreditTransaction.objects.filter(wallet=self.wallet).exists()
                )

    def test_insufficient_credits_raises_and_no_tx_created(self):
        balance = 2
        wallet = Wallet.objects.create(name="Low", balance=balance)
        with self.assertRaises(InsufficientCredits):
            self.reserve_credits(wallet, amount=3)

        wallet.refresh_from_db()
        self.assertEqual(wallet.balance, balance)
        self.assertFalse(CreditTransaction.objects.filter(wallet=wallet).exists())

    def test_idempotency_sequential_returns_same_tx_and_no_double_debit(self):
        key = "idempotency-key"
        amount = 1
        first_tx = self.reserve_credits(self.wallet, amount=amount, idempotency_key=key)
        second_tx = self.reserve_credits(
            self.wallet, amount=amount, idempotency_key=key
        )

        self.wallet.refresh_from_db()
        self.assertEqual(first_tx.id, second_tx.id)  # returned same reservation
        self.assertEqual(
            self.wallet.balance, self.initial_balance - amount
        )  # debited only once
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet_id=self.wallet.id, idempotency_key=key
            ).count(),
            1,
        )

    def test_no_idempotency_key_creates_distinct_reservations(self):
        amount_1 = 2
        amount_2 = 3
        t1 = self.reserve_credits(self.wallet, amount=amount_1)
        t2 = self.reserve_credits(self.wallet, amount=amount_2)

        self.wallet.refresh_from_db()
        self.assertNotEqual(t1.id, t2.id)
        self.assertEqual(
            self.wallet.balance, self.initial_balance - amount_1 - amount_2
        )
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet_id=self.wallet.id, tx_status=TxStatus.PENDING
            ).count(),
            2,
        )

    def test_atomicity_rolls_back_if_tx_insert_fails(self):
        patch_path = (
            "credits.services.reverse_reservation.CreditTransaction.objects.create"
        )
        with patch(patch_path, side_effect=IntegrityError("boom")):
            with self.assertRaises(IntegrityError):
                self.reserve_credits(self.wallet, amount=5)

        # both the decrement and create must roll back
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)
        self.assertFalse(
            CreditTransaction.objects.filter(wallet_id=self.wallet.id).exists()
        )


class ReserveCreditsConcurrencyTests(TransactionTestCase):
    """
    Uses TransactionTestCase so threads actually commit.
    """

    def setUp(self):
        self.wallet = Wallet.objects.create(name="CW", balance=0)
        self.reserve_credits = ReserveCreditsService()

    # Worker that manages per-thread DB lifecycle and returns 1 on success
    def _worker(self, wallet_id: int, amount: int, idempotency_key: str = None):
        try:
            wallet = Wallet.objects.only("id").get(id=wallet_id)
            self.reserve_credits(
                wallet,
                amount=amount,
                idempotency_key=idempotency_key,
                note="concurrency",
            )
            return 1
        except InsufficientCredits:
            return 0
        finally:
            connection.close()

    def test_many_concurrent_reservations_all_succeed_when_funded(self):
        # 200 requests, all amount=1, fund 200
        amounts = [1] * 200
        self.wallet.balance = 200
        self.wallet.save(update_fields=["balance"])

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [
                pool.submit(self._worker, self.wallet.id, amount) for amount in amounts
            ]
            successes = sum(f.result() for f in as_completed(futures))

        self.wallet.refresh_from_db()
        self.assertEqual(successes, 200)
        self.assertEqual(self.wallet.balance, 0)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet_id=self.wallet.id, tx_status=TxStatus.PENDING
            ).count(),
            200,
        )

    def test_concurrent_oversubscription_only_some_succeed_none_overspend(self):
        # 200 attempts with amount=1 but only 50 credits available
        amounts = [1] * 200
        self.wallet.balance = 50
        self.wallet.save(update_fields=["balance"])

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [
                pool.submit(self._worker, self.wallet.id, amount) for amount in amounts
            ]
            successes = sum(f.result() for f in as_completed(futures))

        self.wallet.refresh_from_db()
        self.assertEqual(successes, 50)  # exactly funded count succeeded
        self.assertEqual(self.wallet.balance, 0)  # never negative
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet_id=self.wallet.id, tx_status=TxStatus.PENDING
            ).count(),
            50,
        )

    def test_concurrency_across_multiple_wallets_runs_in_parallel(self):
        # 10 wallets x 20 reservations each
        wallets = [Wallet.objects.create(name=f"W{i}", balance=20) for i in range(10)]

        with ThreadPoolExecutor(max_workers=24) as pool:
            futures = []
            for w in wallets:
                for _ in range(20):
                    futures.append(pool.submit(self._worker, w.id, 1))

            successes = sum(f.result() for f in as_completed(futures))

        self.assertEqual(successes, 200)
        for w in wallets:
            w.refresh_from_db()
            self.assertEqual(w.balance, 0)
            self.assertEqual(
                CreditTransaction.objects.filter(
                    wallet_id=w.id, tx_status=TxStatus.PENDING
                ).count(),
                20,
            )

    def test_concurrent_same_idempotency_key_results_in_single_reservation(self):
        """
        REQUIRES:
        This verifies true idempotency under race.
        """
        self.wallet.balance = 10
        self.wallet.save(update_fields=["balance"])
        key = "idempotency-key-race"

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(self._worker, self.wallet.id, 5, key) for _ in range(2)
            ]
            results = [f.result() for f in as_completed(futures)]

        self.wallet.refresh_from_db()
        # Exactly one reservation should exist and only one debit performed
        self.assertEqual(sum(results), 2)
        self.assertEqual(self.wallet.balance, 5)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet_id=self.wallet.id, idempotency_key=key
            ).count(),
            1,
        )


class CommitReservationTests(TestCase):
    def setUp(self):
        initial_balance = 10
        self.wallet = Wallet.objects.create(name="CR", balance=initial_balance)
        self.reserve_credits = ReserveCreditsService()

    def _pending_tx(self, delta=-1):
        return CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=delta,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.PENDING,
            note="test",
        )

    def test_commit_pending_sets_status_to_committed(self):
        tx = self._pending_tx()
        committed = commit_reservation(tx)

        # Returned and DB state both committed
        self.assertEqual(committed.tx_status, TxStatus.COMMITTED)
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)
        self.assertEqual(tx.pk, committed.pk)

    def test_double_commit_is_idempotent(self):
        tx = self._pending_tx()
        first = commit_reservation(tx)
        second = commit_reservation(tx)  # should return the row unchanged

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(first.tx_status, TxStatus.COMMITTED)
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)

    def test_commit_when_not_pending_returns_unchanged_reversed(self):
        # verify commit is a no-op if the tx is not pending
        reversed_status = TxStatus.REVERSED

        tx = CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=-1,
            tx_type=TxType.DEBIT,
            tx_status=reversed_status,
            note="test",
        )
        same = commit_reservation(tx)

        self.assertEqual(same.pk, tx.pk)
        self.assertEqual(same.tx_status, reversed_status)
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, reversed_status)

    def test_atomicity_rollback_if_save_fails(self):
        # Simulate failure at the save() call inside the atomic block
        tx = self._pending_tx()
        patch_path = "credits.services.commit_reservation.CreditTransaction.save"

        with patch(patch_path, side_effect=IntegrityError("fail")):
            with self.assertRaises(IntegrityError):
                commit_reservation(tx)

        # Status must remain PENDING because the transaction rolled back
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.PENDING)


class CommitReservationConcurrencyTests(TransactionTestCase):
    """
    Uses TransactionTestCase so threads actually commit.
    """

    def setUp(self):
        self.wallet = Wallet.objects.create(name="CW", balance=10)

    def _pending_tx(self, delta=-1):
        return CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=delta,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.PENDING,
            note="conc",
        )

    def _worker_commit_same(self, tx_id: int):
        """
        Worker that tries to commit the SAME tx. Should be idempotent across many threads.
        Returns the final status string.
        """
        try:
            tx = CreditTransaction.objects.get(id=tx_id)
            commited = commit_reservation(tx)

            return commited.tx_status
        finally:
            connection.close()

    def _worker_commit_by_id(self, tx_id: int):
        """
        Worker that commits its own tx id. Returns 1 on success.
        """
        try:
            tx = CreditTransaction.objects.get(id=tx_id)
            commit_reservation(tx)
            return 1
        finally:
            connection.close()

    def test_racing_commit_on_same_tx_is_idempotent(self):
        """
        Many threads race to commit a single PENDING row.
        Exactly one UPDATE happens; all threads should see COMMITTED and no exceptions.
        """
        tx = self._pending_tx()

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(self._worker_commit_same, tx.id) for _ in range(100)]
            results = [f.result() for f in as_completed(futures)]

        # Everyone sees COMMITED
        self.assertTrue(all(s == TxStatus.COMMITTED for s in results))
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)

    def test_bulk_concurrent_commits_across_many_rows(self):
        """
        Commit a large batch of independent PENDING transactions concurrently.
        All should end up COMMITTED.
        """
        # Create 200 pending rows across multiple wallets (speeds up under lock churn)
        wallets = [Wallet.objects.create(name=f"WC{i}", balance=10) for i in range(20)]
        tx_ids = []

        for i, w in enumerate(wallets):
            for _ in range(10):  # 20 * 10 = 200
                tx = CreditTransaction.objects.create(
                    wallet=w,
                    delta=-1,
                    tx_type=TxType.DEBIT,
                    tx_status=TxStatus.PENDING,
                    note="bulk",
                )
                tx_ids.append(tx.id)

        with ThreadPoolExecutor(max_workers=32) as pool:
            futures = [
                pool.submit(self._worker_commit_by_id, tx_id) for tx_id in tx_ids
            ]
            successes = sum(f.result() for f in as_completed(futures))

        self.assertEqual(successes, len(tx_ids))
        committed = CreditTransaction.objects.filter(
            id__in=tx_ids, tx_status=TxStatus.COMMITTED
        ).count()
        self.assertEqual(committed, len(tx_ids))


class ReverseReservationUnitTests(TestCase):
    def setUp(self):
        self.initial_balance = 5
        self.wallet = Wallet.objects.create(name="W", balance=self.initial_balance)
        self.reserve_credits = ReserveCreditsService()

    def _pending_tx(self, amount=1, note="test-pending"):
        # reservation = PENDING DEBIT of 'amount' -> delta = -amount
        return CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=-amount,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.PENDING,
            note=note,
        )

    def test_reverse_pending_creates_refund_updates_balance_and_status(self):
        reason = "timeout"

        # 1) Reserve 3 (this subtracts from balance -> should be 2 now)
        amount = 3
        tx = self.reserve_credits(self.wallet, amount=amount)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - amount)
        self.assertEqual(tx.tx_status, TxStatus.PENDING)

        # 2) Reverse that pending reservation
        reversed_tx = reverse_reservation(tx, reason=reason)

        # Status flipped to REVERSED
        self.assertEqual(reversed_tx.tx_status, TxStatus.REVERSED)
        self.assertEqual(reversed_tx.note, reason)
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.REVERSED)
        self.assertEqual(tx.note, reason)

        # 3) Balance returns to pre-reservation amount (5)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)

        # 4) Refund ledger row created (+3), commited
        refund = CreditTransaction.objects.filter(
            wallet=self.wallet,
            tx_type=TxType.REFUND,
            tx_status=TxStatus.COMMITTED,
            delta=3,
        ).first()
        self.assertIsNotNone(refund)
        self.assertIn(f"refund of tx {tx.id}:", refund.note)

    def test_reverse_is_idempotent_sequential_calls(self):
        amount = 2
        tx = self._pending_tx(amount=amount)
        reverse_reservation(tx, reason="r1")
        reverse_reservation(tx, reason="r2")  # no-op

        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.REVERSED)
        # note should remain the first reason (second call no-ops)
        self.assertEqual(tx.note, "r1")

        # Only one refund exists and balance increased once
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance + amount)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet,
                tx_type=TxType.REFUND,
            ).count(),
            1,
        )

    def test_commit_or_reversed_input_is_noop(self):
        # already COMMITTED
        commited = CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=-1,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.COMMITTED,
            note="comm",
        )
        out1 = reverse_reservation(commited, reason="ignored")

        self.assertEqual(out1.tx_status, TxStatus.COMMITTED)
        self.wallet.refresh_from_db()
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_type=TxType.REFUND
            ).count(),
            0,
        )

        # already REVERSED
        reversed_tx = CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=-1,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.REVERSED,
            note="reversed",
        )
        out2 = reverse_reservation(reversed_tx, reason="ignored")
        self.assertEqual(out2.tx_status, TxStatus.REVERSED)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_type=TxType.REFUND
            ).count(),
            0,
        )

    def test_atomicity_rollback_if_wallet_update_fails(self):
        amount = 2
        tx = self._pending_tx(amount=amount)

        # Patch ONLY the update() to raise -> entire atomic block rolls back
        patch_path = "credits.services.reverse_reservation.Wallet.objects.filter"
        with patch(patch_path, side_effect=IntegrityError("fail update")):
            with self.assertRaises(IntegrityError):
                reverse_reservation(tx, reason="rollback")

        # Status and balance unchanged, no refund row
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.PENDING)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)
        self.assertFalse(
            CreditTransaction.objects.filter(
                wallet=self.wallet,
                tx_type=TxType.REFUND,
            ).exists()
        )

    def test_atomicity_rollback_if_refund_insert_fails(self):
        tx = self._pending_tx(amount=2)

        with patch(
            "credits.services.reverse_reservation.CreditTransaction.objects.create",
            side_effect=IntegrityError("boom"),
        ):
            with self.assertRaises(IntegrityError):
                reverse_reservation(tx, reason="fail-insert")

        # Everything rolled back (including status and balance)
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.PENDING)
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)
        self.assertFalse(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_type=TxType.REFUND
            ).exists()
        )


class ReverseReservationConcurrencyTests(TransactionTestCase):
    """
    Many threads racing on the same PENDING row -> exactly one reverse+refund.
    Bulk parallel reversals across many rows -> all succeed independently.
    Each thread closes its own DB connection to avoid lingering sessions.
    """

    def setUp(self):
        self.wallet = Wallet.objects.create(name="CW", balance=0)

    def _pending_tx(self, amount=1, note="conc"):
        return CreditTransaction.objects.create(
            wallet=self.wallet,
            delta=-amount,
            tx_type=TxType.DEBIT,
            tx_status=TxStatus.PENDING,
            note=note,
        )

    def _worker_same_tx(self, tx_id: int, reason: str):
        try:
            tx = CreditTransaction.objects.get(id=tx_id)
            out = reverse_reservation(tx, reason=reason)

            return out.tx_status
        finally:
            connection.close()

    def _worker_by_id(self, tx_id: int, reason: str):
        try:
            tx = CreditTransaction.objects.get(id=tx_id)
            reverse_reservation(tx, reason=reason)
            return 1
        finally:
            connection.close()

    def test_racing_reverse_on_same_tx_is_idempotent(self):
        """
        100 threads try to reverse the same PENDING tx.
        Exactly one reverse+refund occurs; final note equals the winner's reason.
        """
        # Start with balance 0 so refund effect is measurable
        tx = self._pending_tx(amount=3)
        reasons = [f"r{i}" for i in range(100)]

        with ThreadPoolExecutor(max_workers=24) as pool:
            futures = [
                pool.submit(self._worker_same_tx, tx.id, reasons[i]) for i in range(100)
            ]
            statuses = [f.result() for f in as_completed(futures)]

        self.assertTrue(all(s == TxStatus.REVERSED for s in statuses))
        # Everyone should see REVERSED after the winner flips it
        tx.refresh_from_db()
        self.assertEqual(tx.tx_status, TxStatus.REVERSED)
        self.assertIn(tx.note, set(reasons))  # winner's reason captured

        # Exactly one refund of +3, and balance refrects it
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 3)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet,
                tx_type=TxType.REFUND,
                delta=3,
            ).count(),
            1,
        )

    def test_bulk_concurrent_reversals_across_many_rows(self):
        """
        Create 200 independent PENDING txs across multiple wallets and reverse all concurrently.
        All should become REVERSED and produce exactly one REFUND each.
        """
        wallets = [Wallet.objects.create(name=f"W{i}", balance=0) for i in range(20)]
        tx_ids = []

        for w in wallets:
            for _ in range(10):  # 20 * 10 = 200
                tx = CreditTransaction.objects.create(
                    wallet=w,
                    delta=-1,
                    tx_type=TxType.DEBIT,
                    tx_status=TxStatus.PENDING,
                    note="bulk",
                )
                tx_ids.append(tx.id)

        with ThreadPoolExecutor(max_workers=48) as pool:
            futures = [
                pool.submit(self._worker_by_id, tx_id, f"bulk-reverse")
                for tx_id in tx_ids
            ]
            successes = sum(f.result() for f in as_completed(futures))

        self.assertEqual(successes, len(tx_ids))

        # All reversed, all refunds created, balances updated
        reversed_count = CreditTransaction.objects.filter(
            id__in=tx_ids, tx_status=TxStatus.REVERSED
        ).count()
        self.assertEqual(reversed_count, len(tx_ids))

        refund_count = CreditTransaction.objects.filter(
            tx_type=TxType.REFUND, tx_status=TxStatus.COMMITTED
        ).count()
        self.assertEqual(refund_count, len(tx_ids))

        for w in wallets:
            w.refresh_from_db()
            self.assertEqual(w.balance, 10)  # each wallet had 10 reversals of 1


class SweepStaleReservationsTests(TestCase):
    def setUp(self):
        self.initial_balance = 10
        self.wallet = Wallet.objects.create(name="W", balance=self.initial_balance)
        self.reserve_credits = ReserveCreditsService()

    def _reserve(self, amount: int, *, created_at=None) -> CreditTransaction:
        """
        Use real reservation (debits balance), optionally backdate created_at.
        """
        idempotency_key = uuid.uuid4().hex
        tx = self.reserve_credits(
            self.wallet, amount=amount, idempotency_key=idempotency_key
        )

        if created_at is not None:
            CreditTransaction.objects.filter(id=tx.pk).update(created_at=created_at)
            tx.refresh_from_db()

        return tx

    def test_no_stale_is_noop(self):
        now = timezone.now()
        amount = 3
        self._reserve(amount, created_at=now)
        # cutoff = now - 300s; created_at == now, so not < cutoff

        with patch(
            "credits.services.sweep_stale_reservations.timezone.now", return_value=now
        ):
            sweep_stale_reservations()

        # Still pending; balance still held
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - amount)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet,
                tx_status=TxStatus.PENDING,
            ).count(),
            1,
        )
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_type=TxType.REFUND
            ).count(),
            0,
        )

    def test_reverses_only_stale_and_keeps_fresh(self):
        now = timezone.now()
        cutoff = now - timedelta(seconds=settings.CREDIT_RESERVATION_TTL)

        stale = self._reserve(3, created_at=cutoff - timedelta(seconds=1))
        fresh = self._reserve(2, created_at=cutoff + timedelta(seconds=1))

        # After reservations: 10 - 3 - 2 = 5
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - 3 - 2)

        with patch(
            "credits.services.sweep_stale_reservations.timezone.now", return_value=now
        ):
            sweep_stale_reservations()

        stale.refresh_from_db()
        fresh.refresh_from_db()
        self.wallet.refresh_from_db()

        self.assertEqual(stale.tx_status, TxStatus.REVERSED)
        self.assertEqual(fresh.tx_status, TxStatus.PENDING)
        # Balance returns +3 only (fresh still held) 5 + 3 = 8
        self.assertEqual(self.wallet.balance, 8)

        refund = CreditTransaction.objects.filter(
            wallet=self.wallet,
            tx_type=TxType.REFUND,
            delta=3,
        ).first()

        self.assertIsNotNone(refund)
        self.assertIn(f"refund of tx {stale.id}:", refund.note)

    def test_boundary_exact_cutoff_not_reversed(self):
        now = timezone.now()
        cutoff = now - timedelta(seconds=settings.CREDIT_RESERVATION_TTL)
        edge = self._reserve(
            4, created_at=cutoff
        )  # created_at == cutoff (NOT < cutoff)

        with patch(
            "credits.services.sweep_stale_reservations.timezone.now", return_value=now
        ):
            sweep_stale_reservations()

        edge.refresh_from_db()
        self.assertEqual(edge.tx_status, TxStatus.PENDING)
        # Balance still held: 10 - 4 = 6
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - 4)

    def test_multi_batch_drain_processes_all_in_one_call(self):
        now = timezone.now()
        cutoff = now - timedelta(seconds=settings.CREDIT_RESERVATION_TTL)

        self.wallet.balance = 520
        self.wallet.save(update_fields=["balance"])

        for _ in range(520):
            self._reserve(1, created_at=cutoff - timedelta(seconds=1))

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 0)

        with patch(
            "credits.services.sweep_stale_reservations.timezone.now", return_value=now
        ):
            processed = sweep_stale_reservations(chunk_size=128)

        self.assertEqual(processed, 520)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 520)
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_type=TxType.REFUND, delta=1
            ).count(),
            520,
        )
        self.assertEqual(
            CreditTransaction.objects.filter(
                wallet=self.wallet, tx_status=TxStatus.PENDING
            ).count(),
            0,
        )


class SweepStaleReservationsConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create(name="CW", balance=0)
        self.reserve_credits = ReserveCreditsService()

    def _reserve(self, amount: int, *, created_at) -> CreditTransaction:
        tx = self.reserve_credits(
            self.wallet, amount=amount, idempotency_key=uuid.uuid4().hex
        )
        CreditTransaction.objects.filter(pk=tx.pk).update(created_at=created_at)
        tx.refresh_from_db()

        return tx

    def _worker_sweep(self, now):
        """
        Run sweep in a separate thread with safe DB lifecycle.
        """
        try:
            with patch(
                "credits.services.sweep_stale_reservations.timezone.now",
                return_value=now,
            ):
                sweep_stale_reservations()

            return 1
        finally:
            connection.close()

    def test_skip_locked_row_is_ignored_and_processed_later(self):
        """
        Hold a row lock on one stale reservation; sweep should skip it (skip_locked)
        and reverse the other. After releasing, a second sweep reverses the locked one.
        """
        now = timezone.now()
        cutoff = now - timedelta(seconds=settings.CREDIT_RESERVATION_TTL)

        self.wallet.balance = 4
        self.wallet.save(update_fields=["balance"])

        tx_locked = self._reserve(2, created_at=cutoff - timedelta(seconds=1))
        tx_free = self._reserve(2, created_at=cutoff - timedelta(seconds=1))

        # Begin a transaction and acquire a row lock on tx_locked
        with transaction.atomic():
            # Acquire FOR UPDATE lock via ORM
            CreditTransaction.objects.select_for_update().get(pk=tx_locked.pk)

            # Run sweep in another thread/connection -> should skip locked row
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self._worker_sweep, now)
                future.result()
            # Lock is still held until we exit this 'atomic' block

        tx_locked.refresh_from_db()
        tx_free.refresh_from_db()
        self.wallet.refresh_from_db()

        # Only the free one reversed so far
        self.assertEqual(tx_free.tx_status, TxStatus.REVERSED)
        self.assertEqual(tx_locked.tx_status, TxStatus.PENDING)
        self.assertEqual(self.wallet.balance, 2)

        # Second sweep after releasing the lock should reverse the locked one
        with patch(
            "credits.services.sweep_stale_reservations.timezone.now", return_value=now
        ):
            sweep_stale_reservations()

        tx_locked.refresh_from_db()
        self.wallet.refresh_from_db()
        self.assertEqual(tx_locked.tx_status, TxStatus.REVERSED)
        self.assertEqual(self.wallet.balance, 4)

    def test_two_concurrent_sweeps_reverse_all_stale_once(self):
        """
        Run two sweeps at the same time. Thanks to select_for_update(skip_locked=True)
        and idempotent reverse_reservation, every stale reservation is reversed once.
        """
        now = timezone.now()
        cutoff = now - timedelta(seconds=settings.CREDIT_RESERVATION_TTL)
        initial_balance = 10

        wallets = [
            Wallet.objects.create(name=f"W{i}", balance=initial_balance)
            for i in range(20)
        ]
        tx_ids = []
        for w in wallets:
            for _ in range(10):
                tx = self.reserve_credits(w, amount=1, idempotency_key=uuid.uuid4().hex)
                CreditTransaction.objects.filter(pk=tx.pk).update(
                    created_at=cutoff - timedelta(seconds=1)
                )
                tx_ids.append(tx.pk)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self._worker_sweep, now) for _ in range(2)]
            _ = [f.result() for f in as_completed(futures)]

        # All stale reversed exactly once, all balances restored
        reversed_count = CreditTransaction.objects.filter(
            id__in=tx_ids,
            tx_status=TxStatus.REVERSED,
        ).count()
        self.assertEqual(reversed_count, len(tx_ids))

        refund_count = CreditTransaction.objects.filter(
            tx_type=TxType.REFUND,
            tx_status=TxStatus.COMMITTED,
            delta=1,
        ).count()
        self.assertEqual(refund_count, len(tx_ids))

        for w in wallets:
            w.refresh_from_db()
            self.assertEqual(w.balance, initial_balance)
