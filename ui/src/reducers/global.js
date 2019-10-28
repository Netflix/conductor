import {
  AUTH_AUTHORIZATION_STATUS_CHANGED,
  AUTH_INFO_FAILED,
  AUTH_INFO_SUCCEEDED,
  AUTH_LOGIN_FAILED,
  AUTH_LOGIN_REDIRECT_FAILED,
  AUTH_LOGIN_REDIRECT_SUCCEEDED,
  AUTH_LOGIN_SUCCEEDED,
  AUTH_LOGOUT_FAILED,
  AUTH_LOGOUT_SUCCEEDED,
  AUTH_REFRESH_FAILED,
  AUTH_REFRESH_SUCCEEDED,
  AUTH_USER_DEV,
  AUTH_USER_INACTIVE
} from '../actions/AuthActions';

const initialState = {
  appWidth: '1000px',
  code: null,
  isDev: false,
  isLoggedIn: false,
  isAuthorized: false,
  isAuthenticated: false,
  authorizationStatus: 'none',
  authToken: null,
  expiresIn: '',
  refreshToken: null,
  refreshExpiresIn: '',
  user: {
    name: '',
    preferredUsername: '',
    email: '',
    roles: []
  },
  inActiveTime: ''
};

export default function global(state = initialState, action) {
  switch (action.type) {
    case 'MENU_VISIBLE':
      let width = document.body.clientWidth - 180;
      return Object.assign({}, state, {
        appWidth: width + 'px'
      });

    case AUTH_LOGIN_REDIRECT_SUCCEEDED:
      return Object.assign({}, state, {
        code: action.payload.code,
        isLoggedIn: true
      });

    case AUTH_LOGIN_REDIRECT_FAILED:
      return Object.assign({}, state, {
        code: null,
        isLoggedIn: false
      });

    case AUTH_LOGIN_SUCCEEDED:
      return Object.assign({}, state, {
        isAuthenticated: true,
        authToken: action.payload.authToken,
        expiresIn: action.payload.expiresIn,
        refreshToken: action.payload.refreshToken,
        refreshExpiresIn: action.payload.refreshExpiresIn
      });

    case AUTH_LOGIN_FAILED:
      return Object.assign({}, state, {
        isAuthenticated: false,
        // isAuthorized: false,
        // authorizationStatus: 'none',
        authToken: null,
        expiresIn: '',
        refreshToken: null,
        refreshExpiresIn: '',
        user: {
          name: '',
          preferredUsername: '',
          email: '',
          roles: []
        }
      });

    case AUTH_INFO_SUCCEEDED:
      return Object.assign({}, state, {
        user: {
          name: action.payload.user.name,
          preferredUsername: action.payload.user.preferredUsername,
          email: action.payload.user.email,
          roles: action.payload.user.roles
        },
      });

    case AUTH_INFO_FAILED:
      return Object.assign({}, state, {
        user: {
          name: '',
          preferredUsername: '',
          email: '',
          roles: []
        },
      });

    case AUTH_AUTHORIZATION_STATUS_CHANGED: {
      let newState = Object.assign({}, state, {
        authorizationStatus: action.payload.authorizationStatus
      });

      if (action.payload.authorizationStatus === 'successful') {
        newState.isAuthorized = true;
      } else if (action.payload.authorizationStatus === 'forbidden') {
        newState.isAuthorized = false;
      } else if (action.payload.authorizationStatus === 'none') {
        newState.isAuthorized = false;
      } else if (action.payload.authorizationStatus === 'error') {
        newState.isAuthorized = false;
      }

      return newState;
    }

    case AUTH_LOGOUT_SUCCEEDED:
      return Object.assign({}, state, {
        code: null,
        isLoggedIn: false,
        isAuthenticated: false,
        // isAuthorized: false,
        // authorizationStatus: 'none',
        authToken: null,
        expiresIn: '',
        refreshToken: null,
        refreshExpiresIn: '',
        user: {
          name: '',
          preferredUsername: '',
          email: '',
          roles: []
        }
      });

    case AUTH_LOGOUT_FAILED:
      return Object.assign({}, state, {
        isAuthenticated: false,
        // isAuthorized: false,
        // authorizationStatus: 'none',
        authToken: null,
        expiresIn: '',
        refreshToken: null,
        refreshExpiresIn: '',
        user: {
          name: '',
          preferredUsername: '',
          email: '',
          roles: []
        }
      });

    case AUTH_REFRESH_SUCCEEDED:
      return Object.assign({}, state, {
        isAuthenticated: true,
        authToken: action.payload.authToken,
        expiresIn: action.payload.expiresIn,
        refreshToken: action.payload.refreshToken,
        refreshExpiresIn: action.payload.refreshExpiresIn
      });

    case AUTH_REFRESH_FAILED:
      return Object.assign({}, state, {
        isAuthenticated: false,
        // isAuthorized: false,
        // authorizationStatus: 'none',
        authToken: null,
        expiresIn: '',
        refreshToken: null,
        refreshExpiresIn: '',
        user: {
          name: '',
          preferredUsername: '',
          email: '',
          roles: []
        }
      });

    case AUTH_USER_INACTIVE:
      return Object.assign({}, state, {
        inActiveTime: action.payload.inActiveTime
      });
    case AUTH_USER_DEV:
      return Object.assign({}, state, {
        isDev: action.payload.isDev
      });
    default:
      return state;
  }
}
