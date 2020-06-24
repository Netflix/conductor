import {
  authAuthorizationError,
  authAuthorizationPending,
  authAuthorizationReset,
  authAuthorizationSuccessful,
  authInfoFailed,
  authInfoSucceeded,
  authLoginFailed,
  authLoginSucceeded,
  authLogoutFailed,
  authLogoutSucceeded,
  authRedirectFailed,
  authRedirectSucceeded,
  authRefreshFailed,
  authRefreshSucceeded,
  userIsDev
} from '../actions/AuthActions';

const authTokenKey = "AUTH_TOKEN";
const refreshTokenKey = "REFRESH_TOKEN";
const authExpirationDateKey = "AUTH_EXPIRATION_DATE";
const refreshExpirationDateKey = "REFRESH_EXPIRATION_DATE";

const ROOT_REDIRECT_URL = '#/';

export const USER_AUTHORIZED_ROLES = [
  'deluxe.conductor-ui.admin',
  'deluxe.conductor-ui.developer',
  'deluxe.conductor-ui.viewer'
];

export const USER_AUTHORIZED_ROLES_SET = new Set(USER_AUTHORIZED_ROLES);

const getURLParams = (param) => {
  var results = new RegExp('[?&]' + param + '=([^&#]*)').exec(window.location.href);
  if (results === null) {
    return null;
  } else {
    return decodeURI(results[1]) || 0;
  }
};

const saveRedirectURI = () => {
  var redirectURI = window.location.hash;
  redirectURI = redirectURI.substr(0, redirectURI.lastIndexOf('?'));

  // No need to set for root login url
  if (ROOT_REDIRECT_URL !== redirectURI) {
    sessionStorage.setItem('redirectURI', redirectURI);
  }
};

export const authLogin = (isAuthenticated) => {
  return (dispatch) => {
    const code = getURLParams('code');
    if (code === null && !isAuthenticated) {
      const authTokenVal = getLocalAuthToken();
      const refreshTokenVal = getLocalRefreshToken();

      if (authTokenVal && refreshTokenVal) {
        authUserInfo(authTokenVal)(dispatch);
        setupAuthCheck(refreshTokenVal, getRefreshTokenExpiration())(dispatch);
        dispatch(authLoginSucceeded(authTokenVal, 0, refreshTokenVal, 0));
        var redirectURI = sessionStorage.getItem('redirectURI');
        if (redirectURI != null) {
          window.location.href = '/' + redirectURI;
          sessionStorage.clear();
        }
      } else {
        saveRedirectURI();

        let params = {
          redirectURI: window.location.origin
        };

        fetch('/api/auth/login', {
          method: 'POST',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(params)
        }).then(response => {
          if (response.ok) {
            return response.json();
          }
        }).then(data => {
          if (data) {
            window.location.assign(data.url);
            if (code) {
              dispatch(authRedirectSucceeded(code));
              authToken(code)(dispatch);
            }
          }
        }).catch(error => {
          dispatch(authRedirectFailed(error));
        });
      }
    } else {
      dispatch(authRedirectSucceeded(code));
      authToken(code)(dispatch);
    }
  };
};

const authToken = (code) => (dispatch) => {
  let params = {
    code: code,
    redirectURI: window.location.origin
  };

  fetch('/api/auth/token', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params)
  }).then(response => {
    if (response.ok) {
      return response.json();
    } else {
      setTimeout(() => window.location.href = window.location.origin, 3000);
    }
  }).then(data => {
    if (!!data && !!data.access_token) {
      saveTokensLocally(data.access_token, data.expires_in, data.refresh_token, data.refresh_expires_in);
      authUserInfo(data.access_token)(dispatch);
      setupAuthCheck(data.refresh_token, data.expires_in)(dispatch);
      dispatch(authLoginSucceeded(data.access_token, data.expires_in, data.refresh_token, data.refresh_expires_in));
      window.history.replaceState({}, document.title, "/");
      var redirectURI = sessionStorage.getItem('redirectURI');
      window.location.href = '/' + (redirectURI == null ? '#/' : redirectURI);
      sessionStorage.clear();
    } else {
      throw new Error("Unknown data received");
    }
  }).catch(error => {
    dispatch(authLoginFailed(error));
    dispatch(authAuthorizationReset());
  });
};

export const authLogout = (refreshToken) => (dispatch) => {
  if (!refreshToken) {
    refreshToken = localStorage.getItem(refreshTokenKey);
  }

  let params = {
    refresh_token: refreshToken
  };

  fetch('/api/auth/logout', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params)
  })
    .then(response => {
      if (response.ok) {
        return response.json();
      } else {
        throw new Error("Error while trying to logout");
      }
    })
    .then(data => {
      if (data) {
        removeTokensLocally();
        dispatch(authLogoutSucceeded());
        dispatch(authAuthorizationReset());
        window.location.href = '/Logout.html';
      }
    })
    .catch(error => {
      dispatch(authLogoutFailed(error));
      dispatch(authAuthorizationReset());
    });
};

export const setupInactivityTimer = (refreshToken) => (dispatch) => {
  let timeout = 30 * 60 * 1000;  // after 30 mins of inactivity

  var inactivityTimer;
  const resetTimer = (name) => () => {
    if (inactivityTimer) {
      clearTimeout(inactivityTimer);
    }

    inactivityTimer = setTimeout(() => {
      saveRedirectURI();
      authLogout(refreshToken)(dispatch);
    }, timeout);
  };

  window.onload = resetTimer('window.onload');
  document.onload = resetTimer('document.onload');
  document.onmousemove = resetTimer('document.onmousemove');
  document.onmousedown = resetTimer('document.onmousedown'); // touchscreen presses
  document.ontouchstart = resetTimer('document.ontouchstart');
  document.onclick = resetTimer('document.onclick');    // touchpad clicks
  document.onscroll = resetTimer('document.onscroll');    // scrolling with arrow keys
  document.onkeypress = resetTimer('document.onkeypress');
};

export const getLocalAuthToken = () => {
  var token = localStorage.getItem(authTokenKey);
  var expDate = localStorage.getItem(authExpirationDateKey);

  if (token && expDate) {
    var expDateParsed = Date.parse(expDate);
    if (expDateParsed < Date.now())
      return null;
    return token;
  }
  return null;
};

const getLocalRefreshToken = () => {
  var token = localStorage.getItem(refreshTokenKey);
  var expDate = localStorage.getItem(refreshExpirationDateKey);

  if (token && expDate) {
    var expDateParsed = Date.parse(expDate);
    if (expDateParsed < Date.now())
      return null;
    return token;
  }
  return null;
};

const getRefreshTokenExpiration = () => {
  var expDate = localStorage.getItem(refreshExpirationDateKey);
  var expDateParsed = Date.parse(expDate);

  return (expDateParsed - Date.now()) / 1000;
};

const saveTokensLocally = (authToken, authExp, refreshToken, refreshExp) => {
  var authExpire = new Date(Date.now());
  var refreshExpire = new Date(Date.now());
  authExpire.setSeconds(authExpire.getSeconds() + authExp * 0.9);
  refreshExpire.setSeconds(refreshExpire.getSeconds() + refreshExp * 0.9);
  localStorage.setItem(authTokenKey, authToken);
  localStorage.setItem(refreshTokenKey, refreshToken);
  localStorage.setItem(authExpirationDateKey, authExpire.toISOString());
  localStorage.setItem(refreshExpirationDateKey, refreshExpire.toISOString());
};

const removeTokensLocally = () => {
  localStorage.removeItem(authTokenKey);
  localStorage.removeItem(refreshTokenKey);
  localStorage.removeItem(authExpirationDateKey);
  localStorage.removeItem(refreshExpirationDateKey);
};

const authRefresh = (refreshToken) => (dispatch) => {
  let params = {
    refresh_token: refreshToken
  };

  fetch('/api/auth/refresh', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params)
  }).then(response => {
    if (response.ok) {
      return response.json();
    } else {
      throw new Error("Error while trying to refresh token");
    }
  }).then(data => {
    if (!!data && !!data.access_token) {
      saveTokensLocally(data.access_token, data.expires_in, data.refresh_token, data.refresh_expires_in);
      authUserInfo(data.access_token)(dispatch);
      setupAuthCheck(data.refresh_token, data.expires_in)(dispatch);
      dispatch(authRefreshSucceeded(data.access_token, data.expires_in, data.refresh_token, data.refresh_expires_in));
    } else {
      throw new Error("Unknown data received");
    }
  }).catch(error => {
    dispatch(authRefreshFailed(error));
    dispatch(authAuthorizationReset());
    setTimeout(() => authLogout(refreshToken)(dispatch), 3000);
  });
};

const authUserInfo = (token) => (dispatch) => {
  let params = {
    accessToken: token
  };

  dispatch(authAuthorizationPending());

  fetch('/api/auth/user', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + token
    },
    body: JSON.stringify(params)
  })
    .then(response => {
      if (response.ok) {
        return response.json();
      } else {
        removeTokensLocally();
        dispatch(authAuthorizationReset());
        window.location.href = '/Unauthorized.html';
      }
    })
    .then(data => {
        if (data) {
            const roles = data.roles;
            let userRolesSet = new Set(roles);
            let userRolesIntersection = [...USER_AUTHORIZED_ROLES_SET].filter(role => userRolesSet.has(role));
            if (userRolesIntersection.length > 0) {
                let primary_role;
                for (let item of userRolesSet) {
                    if (item == "deluxe.conductor-ui.admin") {
                        primary_role = "ADMIN";
                    } else if (item == "deluxe.conductor-ui.developer" || item == "deluxe.conductor-ui.viewer") {
                        primary_role = "VIEWER";
                    }
                }
                dispatch(authAuthorizationSuccessful());
                dispatch(authInfoSucceeded(data.name, data.preferred_username, data.email, data.roles, primary_role));
            } else {
                removeTokensLocally();
                dispatch(authAuthorizationReset());
                window.location.href = '/Unauthorized.html';
            }
      } else {
        console.error('User auth failed: No data returned');
        removeTokensLocally();
        dispatch(authAuthorizationReset());
        window.location.href = '/Unauthorized.html';
      }
    })
    .catch(error => {
      removeTokensLocally();
      console.error('User auth failed', error);
      dispatch(authInfoFailed(error));
      dispatch(authAuthorizationError());
      dispatch(authAuthorizationReset());
      window.location.href= '/Unauthorized.html';
    });
};

const setupAuthCheck = (refreshToken, expiresIn) => (dispatch) => {
  let timeout = expiresIn * 1000 * 0.9;  // check before the token expires
  setTimeout(() => authRefresh(refreshToken)(dispatch), timeout);
};

