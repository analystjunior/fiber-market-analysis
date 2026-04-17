/**
 * AuthManager — Supabase Auth + guest gating for FiberMapUSA.
 *
 * Guest limits (unauthenticated):
 *   - Map layers: Fiber Penetration only
 *   - County info: one county pin, then upgrade prompt
 *   - Provider view: AT&T only
 *
 * Any logged-in user gets full access (no payment tiers yet).
 */
(function(global) {
    'use strict';

    // ── Constants ─────────────────────────────────────────────────────────────
    var FREE_LAYERS   = ['penetration'];
    var FREE_PROVIDER = 'AT&T';

    // ── State ─────────────────────────────────────────────────────────────────
    var _session           = null;   // Supabase session object
    var _guestCountyUsed   = false;  // guest has used their 1 free county view
    var _authModalTab      = 'signin'; // 'signin' | 'signup'
    var _upgradeContext    = '';     // what feature triggered the upgrade modal

    // ── Auth helpers ──────────────────────────────────────────────────────────

    function isAuthenticated() {
        return _session != null && _session.user != null;
    }

    function _updateHeaderUI() {
        var signInBtn  = document.getElementById('auth-signin-btn');
        var userBadge  = document.getElementById('auth-user-badge');
        var userEmail  = document.getElementById('auth-user-email');
        var signOutBtn = document.getElementById('auth-signout-btn');

        if (isAuthenticated()) {
            if (signInBtn)  signInBtn.style.display  = 'none';
            if (userBadge)  userBadge.style.display  = '';
            if (userEmail)  userEmail.textContent = _session.user.email || 'Signed in';
        } else {
            if (signInBtn)  signInBtn.style.display  = '';
            if (userBadge)  userBadge.style.display  = 'none';
        }
    }

    function _updateLayerLocks() {
        var btns = document.querySelectorAll('#layer-toggle .toggle-btn');
        btns.forEach(function(btn) {
            var layer = btn.dataset.layer;
            var locked = !AuthManager.canUseLayer(layer);
            btn.classList.toggle('layer-locked', locked);
            btn.setAttribute('aria-disabled', locked ? 'true' : 'false');
        });
    }

    function _updateProviderLocks() {
        var items = document.querySelectorAll('#provider-list .provider-item');
        items.forEach(function(item) {
            var name = item.dataset.provider;
            var locked = !AuthManager.canUseProvider(name);
            item.classList.toggle('provider-locked', locked);
            // Add/remove lock icon
            var existing = item.querySelector('.provider-lock-icon');
            if (locked && !existing) {
                var icon = document.createElement('span');
                icon.className = 'provider-lock-icon';
                icon.textContent = '🔒';
                icon.setAttribute('aria-hidden', 'true');
                item.appendChild(icon);
            } else if (!locked && existing) {
                existing.remove();
            }
        });
    }

    function _onAuthChange() {
        _updateHeaderUI();
        _updateLayerLocks();
        _updateProviderLocks();
        // If just signed in, remove guest county restriction
        if (isAuthenticated()) {
            _guestCountyUsed = false;
        }
    }

    // ── Auth actions ──────────────────────────────────────────────────────────

    function _setAuthError(msg) {
        var el = document.getElementById('auth-error');
        if (el) { el.textContent = msg; el.style.display = msg ? '' : 'none'; }
    }

    function _setAuthLoading(loading) {
        var btn = document.getElementById('auth-submit-btn');
        if (btn) { btn.disabled = loading; btn.textContent = loading ? 'Please wait…' : (
            _authModalTab === 'signin' ? 'Sign In' : 'Create Account'
        ); }
    }

    async function _doSignIn(email, password) {
        _setAuthLoading(true);
        _setAuthError('');
        var sb = DataHandler.getSupabaseClient();
        var result = await sb.auth.signInWithPassword({ email: email, password: password });
        _setAuthLoading(false);
        if (result.error) { _setAuthError(result.error.message); return false; }
        _session = result.data.session;
        _onAuthChange();
        AuthManager.hideModals();
        return true;
    }

    async function _doSignUp(email, password) {
        _setAuthLoading(true);
        _setAuthError('');
        var sb = DataHandler.getSupabaseClient();
        var result = await sb.auth.signUp({ email: email, password: password });
        _setAuthLoading(false);
        if (result.error) { _setAuthError(result.error.message); return false; }
        // If email confirmation is disabled, session is set immediately
        if (result.data.session) {
            _session = result.data.session;
            _onAuthChange();
            AuthManager.hideModals();
        } else {
            _setAuthError('');
            var submitBtn = document.getElementById('auth-submit-btn');
            if (submitBtn) submitBtn.textContent = 'Check your email ✓';
        }
        return true;
    }

    // ── Modal helpers ─────────────────────────────────────────────────────────

    function _showModal(id) {
        var el = document.getElementById(id);
        var backdrop = document.getElementById('auth-backdrop');
        if (el) el.style.display = '';
        if (backdrop) backdrop.style.display = '';
    }

    function _hideModal(id) {
        var el = document.getElementById(id);
        if (el) el.style.display = 'none';
    }

    function _switchAuthTab(tab) {
        _authModalTab = tab;
        _setAuthError('');
        var signinTab  = document.getElementById('auth-tab-signin');
        var signupTab  = document.getElementById('auth-tab-signup');
        var submitBtn  = document.getElementById('auth-submit-btn');
        var titleEl    = document.getElementById('auth-modal-title');
        var switchText = document.getElementById('auth-switch-text');

        if (tab === 'signin') {
            if (signinTab)  signinTab.classList.add('active');
            if (signupTab)  signupTab.classList.remove('active');
            if (submitBtn)  submitBtn.textContent = 'Sign In';
            if (titleEl)    titleEl.textContent = 'Sign In';
            if (switchText) {
                switchText.textContent = '';
                var link = document.createElement('span');
                link.textContent = "Don't have an account? Create one";
                link.className = 'auth-switch-link';
                link.onclick = function() { _switchAuthTab('signup'); };
                switchText.appendChild(link);
            }
        } else {
            if (signinTab)  signinTab.classList.remove('active');
            if (signupTab)  signupTab.classList.add('active');
            if (submitBtn)  submitBtn.textContent = 'Create Account';
            if (titleEl)    titleEl.textContent = 'Create Account';
            if (switchText) {
                switchText.textContent = '';
                var link2 = document.createElement('span');
                link2.textContent = 'Already have an account? Sign in';
                link2.className = 'auth-switch-link';
                link2.onclick = function() { _switchAuthTab('signin'); };
                switchText.appendChild(link2);
            }
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    var AuthManager = {

        init: async function() {
            var sb = DataHandler.getSupabaseClient();

            // Check existing session
            var sessionResult = await sb.auth.getSession();
            _session = sessionResult.data && sessionResult.data.session;

            // Listen for future auth changes
            sb.auth.onAuthStateChange(function(event, session) {
                _session = session;
                _onAuthChange();
            });

            _updateHeaderUI();
            _updateLayerLocks();
            _bindModalEvents();
        },

        isAuthenticated: isAuthenticated,

        // ── Gating checks ──────────────────────────────────────────────────

        canUseLayer: function(layer) {
            if (isAuthenticated()) return true;
            return FREE_LAYERS.indexOf(layer) !== -1;
        },

        canPinCounty: function() {
            if (isAuthenticated()) return true;
            return !_guestCountyUsed;
        },

        recordCountyPin: function() {
            _guestCountyUsed = true;
        },

        canUseProvider: function(name) {
            if (isAuthenticated()) return true;
            return name === FREE_PROVIDER;
        },

        // ── Modal control ───────────────────────────────────────────────────

        showAuthModal: function(tab) {
            _switchAuthTab(tab || 'signin');
            _hideModal('upgrade-modal');
            _showModal('auth-modal');
            setTimeout(function() {
                var emailInput = document.getElementById('auth-email');
                if (emailInput) emailInput.focus();
            }, 50);
        },

        showUpgradeModal: function(context) {
            _upgradeContext = context || '';
            var msgEl = document.getElementById('upgrade-modal-msg');
            var messages = {
                layer:    'Switch to all 8 map layers — demographics, competition, build difficulty, BEAD funding, and more.',
                county:   'Explore unlimited county profiles with full fiber, demographic, competition, and build environment data.',
                provider: 'Access the full provider view for all major fiber, cable, and DSL operators nationwide.',
            };
            if (msgEl) msgEl.textContent = messages[context] || 'Sign in for full access to all features.';
            _hideModal('auth-modal');
            _showModal('upgrade-modal');
        },

        hideModals: function() {
            _hideModal('auth-modal');
            _hideModal('upgrade-modal');
            var backdrop = document.getElementById('auth-backdrop');
            if (backdrop) backdrop.style.display = 'none';
        },

        signOut: async function() {
            var sb = DataHandler.getSupabaseClient();
            await sb.auth.signOut();
            _session = null;
            _guestCountyUsed = false;
            _onAuthChange();
        },

        // Called by buildProviderList after rendering to apply lock state
        applyProviderLocks: _updateProviderLocks,
    };

    // ── Event binding ─────────────────────────────────────────────────────────

    function _bindModalEvents() {
        // Backdrop click → close
        var backdrop = document.getElementById('auth-backdrop');
        if (backdrop) backdrop.addEventListener('click', function() { AuthManager.hideModals(); });

        // Close buttons
        ['auth-modal-close', 'upgrade-modal-close'].forEach(function(id) {
            var btn = document.getElementById(id);
            if (btn) btn.addEventListener('click', function() { AuthManager.hideModals(); });
        });

        // Header sign in / sign out
        var signInBtn  = document.getElementById('auth-signin-btn');
        var signOutBtn = document.getElementById('auth-signout-btn');
        if (signInBtn)  signInBtn.addEventListener('click',  function() { AuthManager.showAuthModal('signin'); });
        if (signOutBtn) signOutBtn.addEventListener('click', function() { AuthManager.signOut(); });

        // Auth tabs
        var tabSignin = document.getElementById('auth-tab-signin');
        var tabSignup = document.getElementById('auth-tab-signup');
        if (tabSignin) tabSignin.addEventListener('click', function() { _switchAuthTab('signin'); });
        if (tabSignup) tabSignup.addEventListener('click', function() { _switchAuthTab('signup'); });

        // Auth form submit
        var form = document.getElementById('auth-form');
        if (form) {
            form.addEventListener('submit', async function(e) {
                e.preventDefault();
                var email = (document.getElementById('auth-email') || {}).value || '';
                var pass  = (document.getElementById('auth-password') || {}).value || '';
                if (!email || !pass) { _setAuthError('Please enter your email and password.'); return; }
                if (_authModalTab === 'signin') {
                    await _doSignIn(email, pass);
                } else {
                    await _doSignUp(email, pass);
                }
            });
        }

        // Upgrade modal CTAs
        var upgradeSignin = document.getElementById('upgrade-signin-btn');
        var upgradeSignup = document.getElementById('upgrade-signup-btn');
        if (upgradeSignin) upgradeSignin.addEventListener('click', function() { AuthManager.showAuthModal('signin'); });
        if (upgradeSignup) upgradeSignup.addEventListener('click', function() { AuthManager.showAuthModal('signup'); });

        // ESC to close
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') AuthManager.hideModals();
        });
    }

    global.AuthManager = AuthManager;

})(typeof window !== 'undefined' ? window : global);
