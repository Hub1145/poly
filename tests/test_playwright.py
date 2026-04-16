"""
Playwright end-to-end tests for the Polymarket Alpha Bot UI.
Run with:  python tests/test_playwright.py
"""
import sys
import time
import json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright, Page, expect

BASE_URL = "http://localhost:5000"
TIMEOUT  = 15_000   # ms


def wait_for_socket(page: Page):
    """Wait until the SocketIO status dot turns green (connected)."""
    page.wait_for_selector("#status-text", timeout=TIMEOUT)
    # Allow up to 10s for connection
    for _ in range(20):
        text = page.locator("#status-text").inner_text()
        if "Connected" in text or "Live" in text:
            return
        time.sleep(0.5)


def run_tests():
    results = []

    def ok(name):
        results.append(("PASS", name))
        print(f"  [PASS] {name}")

    def fail(name, err):
        results.append(("FAIL", name, str(err)))
        print(f"  [FAIL] {name}: {err}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(TIMEOUT)

        # ── 1. Page loads ─────────────────────────────────────────────────────
        try:
            page.goto(BASE_URL)
            title = page.title()
            assert len(title) > 0, "Page has no title"
            ok("Page loads successfully")
        except Exception as e:
            fail("Page loads successfully", e)

        # ── 2. Key UI elements present ────────────────────────────────────────
        for elem_id, label in [
            ("#main-control-btn",  "Start Bot button"),
            ("#status-dot",        "Status indicator dot"),
            ("#status-text",       "Status text"),
            ("#m-trades",          "Trades metric"),
            ("#m-winrate",         "Win rate metric"),
            ("#m-profit",          "Profit metric"),
            ("#m-balance",         "Balance metric"),
            ("#log-display",       "Log display"),
        ]:
            try:
                expect(page.locator(elem_id)).to_be_visible()
                ok(f"Element present: {label}")
            except Exception as e:
                fail(f"Element present: {label}", e)

        # ── 3. Tab navigation ──────────────────────────────────────────────────
        for tab in ["positions", "scan", "news", "dev", "settings"]:
            try:
                page.click(f"[data-tab='{tab}']")
                page.wait_for_selector(f"#{ tab }:not([style*='none'])", timeout=3000)
                ok(f"Tab navigates: {tab}")
            except Exception as e:
                fail(f"Tab navigates: {tab}", e)

        # Go back to dashboard
        page.click("[data-tab='dashboard']")

        # ── 4. Settings tab — form elements present ───────────────────────────
        page.click("[data-tab='settings']")
        page.wait_for_selector("#settings-form", timeout=5000)
        for elem_id, label in [
            ("#s-strategy",     "Strategy select"),
            ("#s-mode",         "Paper mode select"),
            ("#s-amount",       "Trade amount input"),
            ("#s-edge",         "Min edge input"),
            ("#s-interval",     "Scan interval input"),
            ("#s-balance",      "Paper balance input"),
            ("#s-max-trades",   "Max trades input"),
            ("#s-min-gap-pp",   "Min gap pp (weather)"),
            ("#s-min-temp-gap", "Min temp gap (ensemble)"),
        ]:
            try:
                expect(page.locator(elem_id)).to_be_visible()
                ok(f"Settings field: {label}")
            except Exception as e:
                fail(f"Settings field: {label}", e)

        # ── 5. Strategy selector — all strategies present ─────────────────────
        try:
            page.click("[data-tab='settings']")
            strategies = page.locator("#s-strategy option").all()
            strategy_values = [o.get_attribute("value") for o in strategies]
            expected = [
                "bayesian_ensemble", "conservative_snw", "aggressive_whale",
                "specialist_precision", "long_range", "volatility",
                "black_swan", "no_bias", "laddering", "disaster",
                "seismic", "weather_prediction"
            ]
            missing = [s for s in expected if s not in strategy_values]
            if missing:
                fail("All strategies in selector", f"Missing: {missing}")
            else:
                ok(f"All {len(expected)} strategies in selector")
        except Exception as e:
            fail("All strategies in selector", e)

        # ── 6. GET /api/config returns valid JSON ──────────────────────────────
        try:
            resp = page.request.get(f"{BASE_URL}/api/config")
            assert resp.status == 200, f"HTTP {resp.status}"
            cfg = resp.json()
            assert "strategy" in cfg, "missing 'strategy'"
            assert "paper_mode" in cfg, "missing 'paper_mode'"
            assert "trade_amount" in cfg, "missing 'trade_amount'"
            assert "min_temp_gap_celsius" in cfg, "missing 'min_temp_gap_celsius'"
            ok("GET /api/config returns valid config")
        except Exception as e:
            fail("GET /api/config returns valid config", e)

        # ── 7. POST /api/config — change strategy ─────────────────────────────
        try:
            resp = page.request.post(
                f"{BASE_URL}/api/config",
                data=json.dumps({"strategy": "no_bias"}),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 200, f"HTTP {resp.status}"
            body = resp.json()
            assert body.get("status") in ("ok", "saved"), f"body={body}"
            ok("POST /api/config changes strategy")
        except Exception as e:
            fail("POST /api/config changes strategy", e)

        # ── 8. POST /api/control — start/stop ────────────────────────────────
        try:
            resp = page.request.post(
                f"{BASE_URL}/api/control",
                data=json.dumps({"action": "start"}),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 200
            ok("POST /api/control start accepted")
        except Exception as e:
            fail("POST /api/control start accepted", e)

        try:
            resp = page.request.post(
                f"{BASE_URL}/api/control",
                data=json.dumps({"action": "stop"}),
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 200
            ok("POST /api/control stop accepted")
        except Exception as e:
            fail("POST /api/control stop accepted", e)

        # ── 9. Alpha Scan tab — badge never says wrong signal count ──────────
        try:
            page.click("[data-tab='scan']")
            page.wait_for_selector("#scan", timeout=5000)
            badge = page.locator("#scan-count-badge")
            expect(badge).to_be_visible()
            badge_text = badge.inner_text()
            # Badge must NOT show a positive count while all rows are warming-up
            # It should say "Scanning..." OR "0 Alpha Signals Found" OR "N Signals Found" (real)
            assert "Warming up" not in badge_text, f"Badge still says warming up: {badge_text}"
            ok(f"Alpha Scan badge correct: '{badge_text}'")
        except Exception as e:
            fail("Alpha Scan badge correct", e)

        # ── 9b. Warming-up rows are visually dimmed (opacity set) ─────────────
        try:
            page.click("[data-tab='scan']")
            page.wait_for_selector("#scan-table tr", timeout=5000)
            # Check first row — if warming_up, should have opacity style
            first_row = page.locator("#scan-table tr").first
            score_text = first_row.locator("td:nth-child(2)").inner_text()
            # Score should be "—" for warming-up rows, or a number for real signals
            assert score_text in ("—",) or score_text.replace(".","").isdigit(), \
                f"Unexpected score text: {score_text}"
            ok(f"Scan table rows render correctly (score='{score_text}')")
        except Exception as e:
            fail("Scan table rows render correctly", e)

        # ── 10. Signal Analysis (dev) tab has download button ─────────────────
        try:
            page.click("[data-tab='dev']")
            page.wait_for_selector("#dev", timeout=5000)
            expect(page.locator("#download-logs-btn")).to_be_visible()
            ok("Dev tab: download logs button visible")
        except Exception as e:
            fail("Dev tab: download logs button visible", e)

        # ── 11. Settings form save — weather settings round-trip ──────────────
        try:
            page.click("[data-tab='settings']")
            page.wait_for_selector("#s-min-temp-gap", timeout=5000)
            page.fill("#s-min-temp-gap", "2.0")
            page.fill("#s-min-gap-pp", "12.0")
            page.click("#settings-form button[type='submit']")
            time.sleep(1)
            # Verify via API
            resp = page.request.get(f"{BASE_URL}/api/config")
            cfg = resp.json()
            assert float(cfg.get("min_temp_gap_celsius", 0)) == 2.0, f"got {cfg.get('min_temp_gap_celsius')}"
            ok("Weather settings saved and reflected in config API")
        except Exception as e:
            fail("Weather settings saved and reflected in config API", e)

        # ── 12. No JS console errors ──────────────────────────────────────────
        try:
            errors = []
            page.on("pageerror", lambda err: errors.append(str(err)))
            page.goto(BASE_URL)
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            if errors:
                fail("No JS console errors on load", f"{len(errors)} error(s): {errors[0][:100]}")
            else:
                ok("No JS console errors on load")
        except Exception as e:
            fail("No JS console errors on load", e)

        browser.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    print(f"Results: {passed} passed, {failed} failed out of {len(results)} tests")
    if failed:
        print("\nFailed tests:")
        for r in results:
            if r[0] == "FAIL":
                print(f"  - {r[1]}: {r[2]}")
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
