# Full Regression Pass (Pre-Public)

## 0) Use the live site only
- Test on `https://nightsonwallstreet.org` (not `file://`).

## 1) API smoke test (terminal)
```bash
API="https://nows-api.onrender.com"
COOKIES="/tmp/nows-regression-cookies.txt"
STAMP="$(date +%s)"
USER="reg_${STAMP}"
PASS="RegPass_${STAMP}!"
rm -f "$COOKIES"

echo "---- health ----"
curl -sS "$API/api/health"
echo

echo "---- register ----"
curl -sS -c "$COOKIES" -H "Content-Type: application/json" \
  -d "{\"username\":\"$USER\",\"password\":\"$PASS\",\"balance\":1000}" \
  "$API/api/auth/register"
echo

echo "---- session (expect authenticated:true) ----"
curl -sS -b "$COOKIES" "$API/api/auth/session"
echo

echo "---- logout ----"
curl -sS -X POST -b "$COOKIES" "$API/api/auth/logout"
echo

echo "---- session after logout (expect authenticated:false) ----"
curl -sS -b "$COOKIES" "$API/api/auth/session"
echo

echo "---- login ----"
curl -sS -c "$COOKIES" -H "Content-Type: application/json" \
  -d "{\"username\":\"$USER\",\"password\":\"$PASS\"}" \
  "$API/api/auth/login"
echo

echo "---- session after login (expect authenticated:true) ----"
curl -sS -b "$COOKIES" "$API/api/auth/session"
echo
```

### Optional: rate-limit check
```bash
API="https://nows-api.onrender.com"
USER="replace_with_existing_username"
for i in $(seq 1 35); do
  code=$(curl -s -o /tmp/rl.out -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$USER\",\"password\":\"wrong_pass_123\"}" \
    "$API/api/auth/login")
  echo "$i:$code"
done | tail -n 12
```
- Expected: some `429` responses near the end.

## 2) Main gameplay checks
- [ ] First launch account flow works (register/login/guest messaging correct).
- [ ] Username persists after reload/restart.
- [ ] Balance persists after reload/restart.
- [ ] Trading buy/sell still updates cash/shares correctly.
- [ ] Save state still survives browser close/reopen.

## 3) Claims/admin checks
- [ ] Submit a claim from user account.
- [ ] Claim appears in admin pending list.
- [ ] Approve claim; user receives funds.
- [ ] Approved credit is not duplicated on refresh.
- [ ] Admin trigger only visible in day-trading view.
- [ ] Admin unlock requires login + admin role.
- [ ] Trusted device unlock works on same device after refresh.

## 4) Casino checks
- [ ] Casino lobby works and game navigation works.
- [ ] Live wins visible in casino lobby.
- [ ] Live wins hidden inside individual casino games.
- [ ] Poker: center-table spam exploit does not change cards.
- [ ] Keno: winning picks turn green.
- [ ] Blackjack: dealer blackjack resolves automatic loss.
- [ ] Plinko: multipliers align with slots on lower row counts.
- [ ] Plinko easy 16-row center multiplier is `0.5x`.
- [ ] Casino kickout flow still works (overlay + return button).

## 5) Browser/device checks
- [ ] Chrome normal window.
- [ ] Safari normal window.
- [ ] Incognito/private window.
- [ ] Mobile layout sanity pass.

## 6) Post-deploy logs
- [ ] Render logs: no repeated `500`/crash loops after testing.
- [ ] Browser console: no persistent red errors in core flows.

## Release decision
- Ship only when all boxes above are checked.
