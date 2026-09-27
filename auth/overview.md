# Authentication & Authorization: From History to Modern Architecture

> **Authentication (AuthN)** = *Who are you?*
> **Authorization (AuthZ)** = *What are you allowed to do?*

These are two separate problems that get solved by two separate (but connected) systems. Almost every security bug in backend systems comes from conflating the two, so this doc keeps them apart throughout: first the identity problem (history → sessions → JWT → OAuth/OIDC), then the permissions problem (RBAC → ABAC → PBAC → ReBAC), then the practices that hold both together.

Each section follows the same pattern: **what problem existed → what was built to solve it → what new problem that created → what came next.** That chain is the actual story of this field — nothing was invented because it was "better," it was invented because the previous thing broke at some scale or in some new environment.

---

## Table of Contents

1. [A Brief History of Proving Who You Are](#1-a-brief-history-of-proving-who-you-are)
2. [Core Mechanisms: Sessions, Cookies, JWT](#2-core-mechanisms-sessions-cookies-jwt)
3. [Stateful vs Stateless Authentication](#3-stateful-vs-stateless-authentication)
4. [Delegation & Standardization: OAuth 2.0 and OIDC](#4-delegation--standardization-oauth-20-and-oidc)
5. [Choosing the Right Approach](#5-choosing-the-right-approach)
6. [Authorization Models: RBAC, ABAC, PBAC, ReBAC](#6-authorization-models-rbac-abac-pbac-rebac)
7. [Security Best Practices](#7-security-best-practices)
8. [Cheat Sheet](#8-cheat-sheet)

---

## 1. A Brief History of Proving Who You Are

### 1.1 Physical tokens (pre-computing)
Long before software, "authentication" meant a physical object or a shared secret: a seal, a key, a guard who recognized your face, a password spoken at a gate. The model was simple — possession or knowledge of a secret proved identity. This is the ancestor of two ideas still in use today: **something you have** (keys → hardware tokens → phones) and **something you know** (spoken password → typed password).

**Problem:** Doesn't scale past a small group of people who can personally vouch for each other, and secrets are easy to steal, copy, or coerce.

### 1.2 Mainframe passwords (1960s)
The first computer password system is usually credited to MIT's **CTSS (Compatible Time-Sharing System)**, ~1961, built to let multiple users share one mainframe without seeing each other's files. Each user got a password stored in a plaintext file on disk.

**Problem it solved:** Multi-tenancy on a single machine — the system needed *some* way to separate "your session" from "my session."
**New problem it created:** The password file itself became the target. In 1962, a researcher printed the plaintext password file to get more of his own allotted computer time — the first recorded password breach. Storing secrets in plaintext means *anyone* who reads the storage owns every identity in the system.

### 1.3 Hashing (1970s–1990s)
The fix: don't store the password, store a one-way function of it (a **hash**). On login, hash the input and compare to the stored hash. Even if the storage leaks, the raw password shouldn't be recoverable.

**Problem it solved:** Plaintext exposure on storage breach.
**New problem it created:** Early hashes (unsalted MD5/SHA1) are deterministic — the same password always produces the same hash. Attackers precompute huge lookup tables of hash → password (**rainbow tables**) and reverse the hash instantly for any common password.

### 1.4 Salting
Add a random, unique value (a **salt**) per user, hash `password + salt`, and store both the salt and the result. Now two users with the same password get different hashes, and a precomputed rainbow table is useless — the attacker would need one table *per salt*.

**Problem it solved:** Rainbow-table attacks at scale.
**New problem it created:** Salting doesn't slow down brute force — MD5/SHA are *designed to be fast*, which is exactly wrong for password storage. A modern GPU can compute billions of SHA-256 hashes per second, so short/common passwords still fall in minutes even when salted.

### 1.5 Deliberately slow hashing (bcrypt, scrypt, Argon2)
Purpose-built password hashing functions (`bcrypt` 1999, `scrypt` 2009, `Argon2` — winner of the 2015 Password Hashing Competition) are intentionally slow and/or memory-hard, with a tunable "cost factor" that you increase as hardware gets faster.

**Problem it solved:** Brute-force speed. Turns an attacker's billions-of-guesses-per-second into thousands or fewer.
**Remaining problem:** This only protects the *credential store*. It says nothing about how identity is carried across the many HTTP requests a user makes after logging in — HTTP itself has no concept of "the same user asked twice." That's the next problem, and it's a transport problem, not a storage problem.

### 1.6 Public Key Infrastructure (PKI) and asymmetric cryptography
In parallel, a different problem was being solved: **how do two parties who've never met establish trust over an insecure network?** Symmetric cryptography (one shared secret) requires the secret to already be shared safely — a chicken-and-egg problem at internet scale.

Asymmetric cryptography (Diffie–Hellman 1976, RSA 1977) solves this with **key pairs**: a public key anyone can see, and a private key only the owner holds. Data encrypted with the public key can only be decrypted with the private key (and vice versa for signatures). **PKI** is the system of Certificate Authorities (CAs) that vouch "this public key really belongs to this domain/entity," which is what makes HTTPS, TLS, and code signing work.

**Problem it solved:** Secure communication and identity verification between strangers, without a pre-shared secret.
**Why it matters for auth:** PKI underpins TLS (so passwords aren't sniffed in transit), and the same public/private key idea reappears later as the signing mechanism for JWTs and as the basis for passwordless authentication (WebAuthn/passkeys, client certificates).

This is the state of the art by the early 2000s: passwords are hashed properly at rest, transport is encrypted via TLS/PKI. What's left unsolved is: **once you've logged in once, how does the server remember you on request #2 without re-checking the password every time — and how does that work when "the server" is actually 50 servers behind a load balancer?**

---

## 2. Core Mechanisms: Sessions, Cookies, JWT

HTTP is **stateless by design** — each request is independent, with no memory of previous ones. Every "logged in" experience on the web is a workaround bolted onto a protocol that fundamentally doesn't have a concept of "session."

### 2.1 Sessions (the original workaround)

**How it works:**
1. User submits credentials.
2. Server verifies them, creates a **session record** (`session_id → { user_id, roles, expiry, ... }`) and stores it server-side (in memory, a database, or a fast store like Redis).
3. Server sends the client only the `session_id` — an opaque, random, meaningless string.
4. Client attaches that `session_id` on every subsequent request.
5. Server looks up the session record by ID on every request to know who's asking.

```text
POST /login {username, password}
  -> server verifies credentials
  -> server: sessions["sess_9f8a..."] = {userId: 42, expiresAt: ...}
  -> response: Set-Cookie: sid=sess_9f8a...

GET /profile
  Cookie: sid=sess_9f8a...
  -> server: record = sessions.get("sess_9f8a...")
  -> if record and not expired: proceed as user 42
  -> else: 401 Unauthorized
```

**Problem it solved:** Gave HTTP a concept of "the same user across requests," and — crucially — because the actual user data lives only on the server, **revocation is instant**: delete the session record and the user is logged out everywhere, immediately. This is still sessions' biggest advantage.

**New problem it created:**
- **Horizontal scaling:** if you have 10 backend servers behind a load balancer, server B needs to find a session created by server A. Fixes are either *sticky sessions* (route the same user to the same server — fragile, breaks on server restart) or a *shared session store* like Redis (extra infrastructure, extra network hop on every request, a new single point of failure).
- **Cross-domain / cross-service:** sessions are naturally tied to cookies, which are tied to a domain. This gets awkward fast for mobile apps, third-party APIs, and microservices talking to each other.

### 2.2 Cookies (the delivery mechanism)

Cookies aren't an authentication mechanism by themselves — they're the **browser's built-in transport** for attaching a small piece of data (like a session ID or a JWT) automatically to every request to a matching domain, without JavaScript having to do it manually.

Key attributes and the specific problem each one solves:

| Attribute | Problem it solves |
|---|---|
| `HttpOnly` | Blocks JavaScript from reading the cookie, so a cross-site scripting (XSS) bug on the page can't steal the token via `document.cookie`. |
| `Secure` | Cookie is only sent over HTTPS, so it can't be sniffed on plain HTTP. |
| `SameSite=Lax/Strict` | Cookie isn't automatically sent on cross-site requests, which is what makes Cross-Site Request Forgery (CSRF) attacks possible in the first place. |
| `Domain` / `Path` | Scopes exactly which requests the cookie is attached to. |

**Remaining problem:** Cookies are a *browser* concept. Native mobile apps, CLI tools, IoT devices, and server-to-server calls have no cookie jar, so anything cookie-based doesn't naturally extend outside the browser.

### 2.3 JWT — JSON Web Token (the stateless answer)

By the early 2010s, the industry was building distributed systems and public APIs at a scale where "every request does a database/Redis lookup just to know who's asking" was a real bottleneck, and where clients weren't always browsers. **JWT (RFC 7519, 2015)** is the answer: a **self-contained, cryptographically signed token** that carries the user's claims *inside itself*, so the server can verify it with pure computation — no storage lookup required.

**Structure:** `header.payload.signature`, each part base64url-encoded.

```text
header:  {"alg": "HS256", "typ": "JWT"}
payload: {"sub": "42", "role": "admin", "iat": 1735000000, "exp": 1735003600}
signature: HMACSHA256(base64url(header) + "." + base64url(payload), secretKey)

Final token:
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.4f8a...  (one string, sent as Bearer token or cookie)
```

**Verification, server-side, no database call:**
```text
function verify(token, secretOrPublicKey):
    header, payload, signature = split(token, ".")
    expectedSig = sign(header + "." + payload, secretOrPublicKey)
    if signature != expectedSig:      # tampered or forged
        reject()
    if payload.exp < now():           # expired
        reject()
    return payload                     # trusted claims: user id, role, etc.
```

Two signing families, and why you'd pick each:
- **Symmetric (HS256):** one shared secret signs and verifies. Simple, but every service that needs to *verify* the token also has the power to *create* one — fine for a single backend, risky to hand out to many services.
- **Asymmetric (RS256 / ES256):** the auth server signs with a **private key**; any number of other services verify with the corresponding **public key**, which can be distributed freely without giving them signing power. This is what makes JWTs practical across microservices — this is literally the PKI idea from §1.6 reapplied to app-level tokens.

**Problem it solved:** Removes the server-side storage lookup and the shared-store bottleneck; any service holding the public key can independently verify a token, which fits microservices and distributed APIs naturally; works identically for browsers, mobile apps, and machine clients since it's just a string in a header.

**New problems it created:**
- **Revocation is hard.** A session can be deleted instantly. A JWT is valid until it expires, *no matter what* — there's no central "delete" because there's no central record. If a token is stolen, or a user is fired, or a role changes, the old token keeps working until it naturally expires.
- **Size.** A session ID is ~32 bytes. A JWT carrying several claims can be 500+ bytes, sent on *every single request*.
- **It's encoded, not encrypted.** Anyone can base64-decode the payload and read it (they just can't *forge* a valid signature for a modified one). Never put secrets (passwords, card numbers) in a JWT payload.
- **Payload can go stale.** If you baked `role: "user"` into the token at login and the admin promotes them mid-session, the token still says `"user"` until it's reissued.

**The common hybrid fix (used almost everywhere in practice):** short-lived **access token** (JWT, 5–15 minutes) + a longer-lived **refresh token** that *is* stored server-side (so it *can* be revoked). The access token's short life bounds the damage window; the refresh token gives you a revocation point without paying the DB-lookup cost on every request — only on the occasional refresh.

**Mini exercise:** Design the data your login endpoint returns for a "hybrid" scheme: what goes in the JWT payload, what goes in the refresh-token record, and what HTTP mechanism you'd use to deliver each to the client (header vs. cookie) and why.

---

## 3. Stateful vs Stateless Authentication

| | **Stateful (sessions)** | **Stateless (JWT)** |
|---|---|---|
| Where identity lives | Server-side store (DB/Redis) | Inside the token itself |
| Per-request cost | 1 storage lookup | Pure CPU (signature check) |
| Revocation | Instant — delete the record | Hard — must wait for expiry, or add a blocklist (which reintroduces state) |
| Horizontal scaling | Needs a shared store or sticky sessions | Naturally scales — any node with the public key can verify |
| Cross-domain / mobile / M2M | Awkward (cookie-bound) | Natural (just a header) |
| Token size | Small (opaque ID) | Larger (encodes claims) |
| Best fit | Traditional server-rendered web apps, admin panels — anywhere instant logout/revocation matters | Distributed APIs, microservices, mobile backends, anywhere you want to avoid a shared-state bottleneck |

**The real answer in production is rarely pure-either:** most serious systems run stateless JWTs for speed *and* keep one small piece of state — a refresh-token table or a short-TTL revocation blocklist — specifically to buy back revocation. That's the actual industry-standard architecture, not a purist stateless system.

---

## 4. Delegation & Standardization: OAuth 2.0 and OIDC

### 4.1 The problem OAuth was built for
Early "integrations" worked like this: *Company A* wants to read your *Company B* contacts, so it just asks for your *Company B* username and password, stores them, and logs in as you. This is the **password anti-pattern** — a third party now holds your actual credentials, with full access, indefinitely, with no way for you to grant *partial* access or revoke just that one integration without changing your password everywhere.

### 4.2 OAuth 2.0 (RFC 6749, 2012) — delegated, scoped access
OAuth defines four roles:
- **Resource Owner** — the user.
- **Client** — the app requesting access (e.g., a calendar app).
- **Authorization Server** — issues tokens after the user consents (e.g., Google's login server).
- **Resource Server** — the API holding the actual data (e.g., Google Calendar API).

The core idea: the client never sees the user's password. Instead, the user is redirected to the Authorization Server, logs in *there*, approves a specific **scope** (e.g., "read your calendar," not "do anything as you"), and the Authorization Server hands the client a short-lived **access token** the client presents to the Resource Server.

**Authorization Code flow (the standard web flow, simplified):**
```text
1. Client redirects user to:
   AUTH_SERVER/authorize?client_id=X&redirect_uri=Y&scope=read_calendar&state=random123

2. User logs in at the Authorization Server, approves scope "read_calendar"

3. Authorization Server redirects back:
   REDIRECT_URI?code=abc123&state=random123

4. Client's BACKEND exchanges the code (server-to-server, not visible to browser):
   POST AUTH_SERVER/token {code: abc123, client_id, client_secret}
   -> {access_token: "...", refresh_token: "...", expires_in: 3600}

5. Client calls the Resource Server:
   GET RESOURCE_SERVER/calendar
   Authorization: Bearer <access_token>
```

**PKCE (Proof Key for Code Exchange)** extends this for clients that can't safely hold a secret — mobile and single-page apps — by having the client prove it's the same party that started the flow, without needing a stored `client_secret`. It's now recommended for *all* clients, not just public ones.

Other grant types exist for other shapes of problem: **Client Credentials** (no user involved — service-to-service, e.g. a backend job calling another backend's API as itself) and the now-deprecated **Resource Owner Password Credentials** and **Implicit** grants (both were early attempts that reintroduced problems OAuth was meant to solve, and are avoided in modern implementations).

**Problem it solved:** Third-party access without credential sharing; scoped, revocable, time-limited permissions instead of all-or-nothing.

**New problem it created:** OAuth 2.0 was designed for *authorization* (access to an API), not *identity*. But because it produces a token after a login screen, people started (mis)using it for "Sign in with Google" anyway — checking "did I get a valid access token back" as a proxy for "the user is who they say they are." This is fragile: an access token proves you can call an API, it says nothing standardized about *who the user is* or *when they last authenticated*, and every provider exposed user-info differently. There was no common contract.

### 4.3 OIDC — OpenID Connect (2014) — identity, standardized
OIDC is a thin identity layer specified on top of OAuth 2.0. It adds:
- An **ID Token** — a JWT, specifically about *authentication*, not API access, with standardized claims (`sub` = stable user ID, `email`, `name`, `iss`, `aud`, `auth_time`, `exp`, ...).
- A standard **`/userinfo`** endpoint to fetch profile data.
- A **discovery document** (`/.well-known/openid-configuration`) so any client can find an issuer's endpoints and public keys automatically.

**Key distinction that trips people up:**
| | Access Token | ID Token |
|---|---|---|
| Purpose | "Here's what this client is allowed to call" | "Here's who logged in, and when" |
| Audience | The Resource Server (API) | The Client application itself |
| Should the client read its contents? | No — treat as opaque, just forward it | Yes — this is the client's proof of identity |

**Problem it solved:** A single, standardized way to say "this app authenticated this user" — this is the actual mechanism behind every "Sign in with Google/Microsoft/Apple" button.

**Remaining problem, honestly:** OAuth/OIDC assumes trust in the Authorization Server and a working redirect-based browser flow; it doesn't remove the earlier revocation problem for the tokens it issues (still solved the hybrid way, §2.3), and running your *own* Authorization Server correctly (token signing, key rotation, redirect URI validation, PKCE) is enough surface area that most teams use a managed identity provider (Auth0, Okta, Keycloak, Cognito, Clerk, etc.) rather than hand-rolling one.

---

## 5. Choosing the Right Approach

| Scenario | Recommended approach | Why |
|---|---|---|
| Traditional server-rendered web app, single backend | **Sessions** | Instant revocation matters, and you don't have a distributed-lookup problem to solve. |
| Distributed APIs / microservices / mobile backend | **Stateless JWT** (+ refresh token) | No shared-store bottleneck; any service can verify independently with the public key. |
| Letting a third-party app act on a user's behalf | **OAuth 2.0** (+ OIDC if you need identity, not just access) | Scoped, revocable, no credential sharing. |
| Service-to-service / machine-to-machine, no user involved | **API keys** or **OAuth Client Credentials** or **mTLS** | There's no "user" to redirect through a login screen; you need a long-lived or automatically-renewed machine identity instead. |

A pragmatic rule of thumb: **default to sessions until you have an actual, measured reason to go stateless** (multiple services needing independent verification, mobile/native clients, or a genuinely large horizontal-scaling need). Statelessness trades away your easiest revocation story, so it should be a deliberate trade, not a default.

---

## 6. Authorization Models: RBAC, ABAC, PBAC, ReBAC

Authentication tells you *who* is calling. Authorization decides, on every single request, whether *that person* can do *this specific thing* to *this specific resource*. This is where most production security bugs actually live — not in the login form, but in a missing check on some endpoint deep in the app ("Insecure Direct Object Reference" — user A requests resource ID belonging to user B, and the backend never checks ownership).

The four models below aren't strict generations replacing each other — they're points on a spectrum from *simple and coarse* to *flexible and fine-grained*, and real systems often combine two of them.

### 6.1 RBAC — Role-Based Access Control

**Idea:** Permissions attach to **roles**, not directly to users. Users get one or more roles.

```text
roles = {
  "admin": ["read:*", "write:*", "delete:*"],
  "editor": ["read:posts", "write:posts"],
  "viewer": ["read:posts"]
}
users = {
  "user_42": ["editor"]
}

function can(userId, action):
    for role in users[userId]:
        if action in roles[role]:
            return true
    return false

# check before a request executes:
if not can("user_42", "delete:posts"):
    return 403 Forbidden
```

**Problem it solved:** Before RBAC, permissions were often assigned per-user directly — fine for 10 users, unmanageable for 10,000. RBAC means onboarding someone is "assign a role," not "individually configure 40 permission flags."

**Problem it doesn't solve (why the next models exist):** RBAC has no concept of *context*. It can express "editors can edit posts," but not "editors can edit *their own* posts, but not someone else's" — that distinction needs to look at a relationship between the user and the specific resource, which a static role can't encode. Teams solve this by creating more and more granular roles (`editor-own-posts-only`), which leads to **role explosion**: hundreds of near-duplicate roles that are hard to audit.

**Assignment:** Extend the pseudocode above so `editor` can edit a post only if `post.authorId == userId`. Notice you can't express this with roles alone — you need to look at the *resource*, which is exactly what §6.2 formalizes.

### 6.2 ABAC — Attribute-Based Access Control

**Idea:** Decisions are computed from **attributes** of the user, the resource, the action, and the environment — evaluated at request time, not looked up from a static table.

```text
function can(user, resource, action, env):
    if action == "edit" and resource.type == "post":
        return user.id == resource.authorId
               and resource.status != "locked"
               and env.time.hour between 6 and 22   # e.g., no off-hours edits
    if action == "view" and resource.type == "report":
        return user.department == resource.department
               or user.clearanceLevel >= resource.sensitivityLevel
    return false
```

**Problem it solved:** Dynamic, contextual decisions RBAC structurally can't express — ownership, time of day, IP range, resource sensitivity, department match, device trust level, anything computable at request time.

**New problem it created:** Logic like this scattered across application code becomes very hard to audit ("what are *all* the ways someone can access a sensitive report?" now requires reading code, not reading a table) and hard to reuse consistently across services — every team reimplements similar attribute checks slightly differently.

**Assignment:** Write the attribute rule for "a support agent can view a customer's ticket only if the ticket is assigned to their team AND was created in the last 90 days." Identify which attributes belong to the user, which to the resource, and which to the environment.

### 6.3 PBAC — Policy-Based Access Control

**Idea:** Take the ABAC logic *out* of scattered application code and centralize it as **declarative policy**, evaluated by a dedicated policy engine that every service calls. The most common real implementation of this idea is **OPA (Open Policy Agent)** with its policy language **Rego**, or AWS IAM policies as another familiar example.

```text
# policy (language-agnostic pseudocode of a Rego-style rule)
policy "edit_post":
    allow if:
        request.action == "edit"
        request.resource.type == "post"
        request.user.id == request.resource.authorId
        request.resource.status != "locked"

# application code becomes a single, uniform call:
decision = policyEngine.evaluate(policy="edit_post", request=currentRequest)
if not decision.allow:
    return 403 Forbidden
```

**Problem it solved:** Decouples "what's the rule" from "where's the code that runs it" — policies become versioned, testable, centrally auditable artifacts instead of `if` statements buried across a codebase. Multiple services enforce the *same* policy by calling the *same* engine instead of each re-implementing the logic.

**New problem it created:** Now you have a new critical-path dependency (the policy engine) that every authorization check calls — its latency and availability directly become your API's latency and availability, and you've added an entirely new domain-specific language (Rego, Cedar, etc.) for the team to learn.

**Assignment:** Take the ABAC rule you wrote in §6.2's exercise and rewrite it as a standalone policy definition, separate from any specific programming language — then describe, in one sentence, what your API code would send to the policy engine and what it would get back.

### 6.4 ReBAC — Relationship-Based Access Control

**Idea:** Permissions are derived from a **graph of relationships** between entities, not from roles or static attributes. "Can user X view document Y?" becomes a graph-reachability question: is there a path from X to Y through relationships like `owner`, `editor`, `member-of`, `parent-folder`? This is the model behind **Google's Zanzibar** paper (2019, the system behind Drive/Docs sharing permissions) and open-source implementations like **SpiceDB** and **Ory Keto**.

```text
# relationship tuples, e.g.:
(user:42,   "owner",  document:100)
(user:43,   "editor", document:100)
(group:eng, "viewer", document:100)
(user:44,   "member", group:eng)

function can(user, action, resource):
    if action == "view":
        return hasRelation(user, ["owner","editor","viewer"], resource)
                or existsGroup g where hasRelation(g, "viewer", resource)
                                    and hasRelation(user, "member", g)
    if action == "edit":
        return hasRelation(user, ["owner","editor"], resource)
```

This is exactly how "share this folder with the Engineering group, and anyone in that group can see every file inside it, including subfolders" works in real products — it's relationships composed through a graph, not a role or a static attribute rule.

**Problem it solved:** ABAC/PBAC struggle to express *inherited, nested, and transitive* access naturally (a file inside a folder inside a shared drive; a group that's a member of another group). ReBAC models this as first-class graph traversal, which is a much more natural fit and — in systems like Zanzibar — can be made consistently fast at massive scale.

**New problem it introduces:** You need a graph store and a traversal engine (not just a rules table), consistency guarantees become genuinely tricky at scale (Zanzibar's paper spends significant effort on exactly this — making sure a permission change is visible everywhere before a subsequent read, without every read paying a huge latency cost), and reasoning about "who can see this" for an audit can require walking a large graph rather than reading one row.

**Assignment:** Model a simple "shared folders" feature as relationship tuples: a user owns a folder, invites another user as an editor, and a whole team gets viewer access. Write out the tuples, then write the `can(user, "view", file)` check assuming the file inherits its folder's permissions.

### 6.5 Putting it together
Most real systems don't pick exactly one. A common pragmatic combination: **RBAC for coarse gates** ("is this user even an employee vs. a customer, is this account on the right subscription tier") layered with **ReBAC or ABAC for resource-level, ownership-aware decisions** ("can this specific employee edit this specific document"), often implemented through a **PBAC-style centralized engine** so the rules are auditable in one place instead of scattered across services.

---

## 7. Security Best Practices

### 7.1 Don't leak information through error messages
A login form that says "no account with that email" vs. "incorrect password" tells an attacker which emails are registered — an **enumeration attack**. Always return the same generic message and the same timing profile for both cases:

```text
# BAD
if not userExists(email): return "No account found"
if not passwordMatches(email, password): return "Incorrect password"

# GOOD — identical response either way
if not userExists(email) or not passwordMatches(email, password):
    return "Invalid email or password"
```

The same principle applies past login: a `404` for "resource doesn't exist" and a `404` for "resource exists but you're not authorized to see it" should usually look identical from the outside — returning `403` instead of `404` can itself leak that the resource exists.

### 7.2 Timing attacks and constant-time comparison
A naive string comparison (`a == b`) typically returns as soon as it hits the first mismatched character — which means comparing a *closer* guess takes measurably longer than comparing a wildly wrong one. Over many requests, an attacker can use that timing difference to guess a secret (like an API key or token) one character at a time.

The fix is a **constant-time comparison** function that always takes the same amount of time regardless of where — or whether — a mismatch occurs, usually by comparing every byte and combining the result with a bitwise OR/XOR instead of returning early:

```text
function constantTimeEquals(a, b):
    if length(a) != length(b):
        return false          # length itself should also not leak timing where possible
    result = 0
    for i in 0..length(a):
        result = result OR (byteAt(a, i) XOR byteAt(b, i))
    return result == 0
```

Nearly every mainstream language/standard library ships one of these already (e.g. `crypto/subtle.ConstantTimeCompare` in Go, `hmac.compare_digest` in Python, `crypto.timingSafeEqual` in Node) — the point isn't to hand-roll it, it's to know *when* you need it: comparing secrets (tokens, HMAC signatures, API keys), never comparing regular non-secret data.

### 7.3 Other practices worth naming
- **Rate limit and lock out** login and token-refresh endpoints to blunt brute-force and credential-stuffing attacks.
- **Short-lived access tokens + revocable refresh tokens** (the hybrid from §2.3) so a leaked access token has a small blast radius.
- **Never store JWTs in `localStorage`** for anything security-sensitive — it's readable by any JavaScript on the page, so an XSS bug becomes full account takeover. Prefer an `HttpOnly` cookie.
- **Rotate signing keys** periodically and support key rotation without downtime (this is what the `kid` header field in a JWT and the discovery document in OIDC are for).
- **MFA** (a second, independent factor — TOTP, WebAuthn/passkeys) so a leaked password alone isn't enough.
- **Principle of least privilege** at every authorization layer above — grant the narrowest scope/role/relationship that gets the job done, not the broadest one that's convenient.

---

## 8. Cheat Sheet

| Need | Reach for |
|---|---|
| Traditional web app, need instant logout | Sessions |
| Distributed system, many services verifying tokens | JWT (short-lived) + server-side refresh token |
| Let a third-party app access user data | OAuth 2.0 |
| "Sign in with Google/GitHub/etc." | OIDC (built on OAuth 2.0) |
| Machine-to-machine, no human user | API key / OAuth Client Credentials / mTLS |
| Simple, small permission set (admin/editor/viewer) | RBAC |
| Permissions depend on context (ownership, time, department) | ABAC |
| Need one auditable, centrally-enforced rule set across services | PBAC (e.g. OPA/Rego) |
| Sharing/nested/graph-like permissions (folders, groups, teams) | ReBAC (e.g. Zanzibar-style) |
| Comparing any secret value | Constant-time comparison, always |
