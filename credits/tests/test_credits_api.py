import uuid

from django.http import JsonResponse
from django.test import Client, RequestFactory, TestCase
from django.urls import reverse

from ..decorators import charge_one_credit
from ..enums import TxStatus, TxType
from ..middleware import ApiKeyMiddleware
from ..models import ApiKey, CreditTransaction, Wallet


class BalanceApiTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.url = reverse("credits:balance")

        self.initial_balance = 10
        self.wallet = Wallet.objects.create(name="W-main", balance=self.initial_balance)
        self.active_key = ApiKey.objects.create(wallet=self.wallet, key="k_active")
        self.inactive_key = ApiKey.objects.create(
            wallet=self.wallet, key="k_inactive", is_active=False
        )

        # second wallet to ensure we don't cross wires
        self.wallet_2 = Wallet.objects.create(name="W-2", balance=99)
        self.active_key_2 = ApiKey.objects.create(
            wallet=self.wallet_2, key="k_active_2"
        )

    def test_missing_api_key_returns_401(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"detail": "API key required"})

    def test_blank_api_key_returns_401(self):
        response = self.client.get(self.url, HTTP_X_API_KEY="")
        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"detail": "API key required"})

    def test_invalid_api_key_returns_401(self):
        response = self.client.get(self.url, HTTP_X_API_KEY="not-a-real-key")
        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"detail": "API key required"})

    def test_inactive_api_key_returns_401(self):
        response = self.client.get(self.url, HTTP_X_API_KEY=self.inactive_key.key)
        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"detail": "API key required"})

    def test_valid_api_key_returns_wallet_name_and_balance(self):
        response = self.client.get(self.url, HTTP_X_API_KEY=self.active_key.key)
        self.assertEqual(response.status_code, 200)

        self.assertJSONEqual(
            response.content,
            {"wallet": self.wallet.name, "balance": self.wallet.balance},
        )

    def test_valid_api_key_for_different_wallet_is_isolated(self):
        # Ensure the correct wallet is attached by the middleware
        response = self.client.get(self.url, HTTP_X_API_KEY=self.active_key.key)
        response_2 = self.client.get(self.url, HTTP_X_API_KEY=self.active_key_2.key)

        self.assertJSONEqual(
            response.content,
            {"wallet": self.wallet.name, "balance": self.wallet.balance},
        )

        self.assertJSONEqual(
            response_2.content,
            {"wallet": self.wallet_2.name, "balance": self.wallet_2.balance},
        )

    def test_valid_request_is_single_query_total(self):
        with self.assertNumQueries(1):
            resp = self.client.get(self.url, HTTP_X_API_KEY=self.active_key.key)
        self.assertEqual(resp.status_code, 200)

    def test_balance_reflects_latest_value(self):
        resp = self.client.get(self.url, HTTP_X_API_KEY=self.active_key.key)
        self.assertJSONEqual(
            resp.content, {"wallet": self.wallet.name, "balance": self.wallet.balance}
        )

        bonus = 100
        self.wallet.balance = self.wallet.balance + bonus
        self.wallet.save(update_fields=["balance"])

        resp2 = self.client.get(self.url, HTTP_X_API_KEY=self.active_key.key)
        self.assertJSONEqual(
            resp2.content,
            {"wallet": self.wallet.name, "balance": self.initial_balance + bonus},
        )


class EchoApiTests(TestCase):
    def setUp(self):
        self.client = Client()

        self.initial_balance = 10
        self.wallet = Wallet.objects.create(name="W", balance=self.initial_balance)
        self.key_active = ApiKey.objects.create(
            wallet=self.wallet, key="k_active", is_active=True
        )
        self.key_inactive = ApiKey.objects.create(
            wallet=self.wallet, key="k_inactive", is_active=False
        )

        self.initial_balance2 = 99
        self.wallet2 = Wallet.objects.create(name="W2", balance=99)
        self.key_active2 = ApiKey.objects.create(
            wallet=self.wallet2, key="k2", is_active=True
        )

        self.url = reverse("credits:echo")

        self.rf = RequestFactory()
        # middleware used only to attach request.wallet
        self.mw = ApiKeyMiddleware(lambda req: JsonResponse({"_": "through"}))

    def test_missing_api_key_401(self):
        resp = self.client.get(self.url)
        self.assertEqual(resp.status_code, 401)
        self.assertJSONEqual(resp.content, {"detail": "API key required"})
        self.assertEqual(CreditTransaction.objects.count(), 0)

    def test_invalid_api_key_401(self):
        resp = self.client.get(self.url, HTTP_X_API_KEY="nope")
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(CreditTransaction.objects.count(), 0)

    def test_inactive_api_key_401(self):
        resp = self.client.get(self.url, HTTP_X_API_KEY=self.key_inactive.key)
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(CreditTransaction.objects.count(), 0)

    def test_happy_path_commits_one_credit(self):
        start = self.wallet.balance
        idempotency_key = uuid.uuid4()

        response = self.client.get(
            self.url,
            HTTP_X_API_KEY=self.key_active.key,
            HTTP_IDEMPOTENCY_KEY=idempotency_key,
        )
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(
            response.content, {"ok": True, "message": "Service did its job!"}
        )

        # Ledger: exactly one COMMITED debit of -1 for this wallet/key
        txs = CreditTransaction.objects.filter(
            wallet=self.wallet, idempotency_key=idempotency_key
        )
        self.assertEqual(txs.count(), 1)

        tx = txs.first()
        self.assertEqual(tx.tx_type, TxType.DEBIT)
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)
        self.assertEqual(tx.delta, -1)

        # Balance decreased by 1
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, start - 1)

    def test_isolation_across_wallets(self):
        r1 = self.client.get(
            self.url,
            HTTP_X_API_KEY=self.key_active.key,
            HTTP_IDEMPOTENCY_KEY=uuid.uuid4().hex,
        )
        r2 = self.client.get(
            self.url,
            HTTP_X_API_KEY=self.key_active2.key,
            HTTP_IDEMPOTENCY_KEY=uuid.uuid4().hex,
        )
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))

        self.wallet.refresh_from_db()
        self.wallet2.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - 1)
        self.assertEqual(self.wallet2.balance, self.initial_balance2 - 1)

    def test_idempotency_same_key_charges_once(self):
        idem = "repeat-key"

        r1 = self.client.get(
            self.url, HTTP_X_API_KEY=self.key_active.key, HTTP_IDEMPOTENCY_KEY=idem
        )
        r2 = self.client.get(
            self.url, HTTP_X_API_KEY=self.key_active.key, HTTP_IDEMPOTENCY_KEY=idem
        )
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))

        # Only one ledger row for that key, COMMITTED
        txs = CreditTransaction.objects.filter(wallet=self.wallet, idempotency_key=idem)
        self.assertEqual(txs.count(), 1)

        tx = txs.first()
        self.assertEqual(tx.tx_status, TxStatus.COMMITTED)

        # Balance only decremented once
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance - 1)

    def test_insufficient_credits_returns_402_and_no_persisted_tx(self):
        self.wallet.balance = 0
        self.wallet.save(update_fields=["balance"])
        idem = uuid.uuid4().hex

        response = self.client.get(
            self.url, HTTP_X_API_KEY=self.key_active.key, HTTP_IDEMPOTENCY_KEY=idem
        )
        self.assertEqual(response.status_code, 402)
        self.assertJSONEqual(response.content, {"detail": "Insufficient credits"})

        # No ledger row (service rolls back on InsufficientCredits)
        self.assertFalse(
            CreditTransaction.objects.filter(
                wallet=self.wallet, idempotency_key=idem
            ).exists()
        )
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, 0)

    def _attach_wallet(self, req):
        # run middleware once to mutate request.wallet; ignore response
        _ = self.mw(req)
        return req

    def test_non_2xx_response_reverses_and_refunds(self):
        idem = uuid.uuid4().hex

        @charge_one_credit()
        def stub_bad(request):
            return JsonResponse({"ok": False}, status=400)

        request = self.rf.get(
            "/stub",
            HTTP_X_API_KEY=self.key_active.key,
            HTTP_IDEMPOTENCY_KEY=idem,
        )
        request = self._attach_wallet(request)

        response = stub_bad(request)
        self.assertEqual(response.status_code, 400)

        # One PENDING debit should have been REVERSED; one REFUND created; net 0 change
        debit = CreditTransaction.objects.filter(
            wallet=self.wallet, idempotency_key=idem, tx_type=TxType.DEBIT
        ).first()
        self.assertIsNotNone(debit)
        self.assertEqual(debit.tx_status, TxStatus.REVERSED)

        refund = CreditTransaction.objects.filter(
            wallet=self.wallet, tx_type=TxType.REFUND, delta=1
        ).first()
        self.assertIsNotNone(refund)
        self.assertEqual(refund.tx_status, TxStatus.COMMITTED)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)

    def test_exception_reverses_and_refunds_then_bubbles(self):
        idem = uuid.uuid4().hex

        # stub view that raises; decorator must reverse+refund and re-raise
        @charge_one_credit()
        def stub_crash(request):
            raise RuntimeError("boom")

        request = self.rf.get(
            "/stub",
            HTTP_X_API_KEY=self.key_active.key,
            HTTP_IDEMPOTENCY_KEY=idem,
        )
        request = self._attach_wallet(request)

        with self.assertRaises(RuntimeError):
            _ = stub_crash(request)

        debit = CreditTransaction.objects.filter(
            wallet=self.wallet, idempotency_key=idem, tx_type=TxType.DEBIT
        ).first()
        self.assertIsNotNone(debit)
        self.assertEqual(debit.tx_status, TxStatus.REVERSED)

        refund = CreditTransaction.objects.filter(
            wallet=self.wallet, tx_type=TxType.REFUND, delta=1
        ).first()
        self.assertIsNotNone(refund)
        self.assertEqual(refund.tx_status, TxStatus.COMMITTED)

        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, self.initial_balance)
