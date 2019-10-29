import axios from 'axios';
import KJUR from 'jsrsasign';
import DnsResolver from './DnsResolver';
import {serverConfig} from './ServerConfig';

const authServiceName = serverConfig.authServiceName();
const keycloakServiceUrl = serverConfig.keycloakServiceUrl();
const keycloakServiceName = serverConfig.keycloakServiceName();

const client_id = serverConfig.clientId();
const client_secret = serverConfig.clientSecret();

const AuthClient = {

  resolveReqServiceHost(serviceName, success, error) {
    const host = serviceName;
    new DnsResolver().resolve(host, results => {
      if (results && results.length > 0)
        success(results[0]);
      else
        error(`Dns lookup failed for host ${host}. No matches found.`);
    }, err => error(err));
  },

  // gets an auth token
  login(redirectURI, success, error) {
    let config = {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    };

    let keycloakLoginUri = '/auth/realms/deluxe/protocol/openid-connect/auth?client_id=' + client_id
      + '&response_type=code&redirect_uri=' + encodeURIComponent(redirectURI);

    if (keycloakServiceUrl) {
      axios.get(keycloakServiceUrl + keycloakLoginUri, config)
        .then(response => {
          success(keycloakServiceUrl + keycloakLoginUri);
        }).catch(err => {
        error(err);
      });
    } else {

      this.resolveReqServiceHost(keycloakServiceName, host => {
        let keycloakLoginHost = `http://${host.name}:${host.port}`;
        axios.get(keycloakLoginHost + keycloakLoginUri, config)
          .then(response => {
            success(keycloakLoginHost + keycloakLoginUri);
          })
          .catch(err => {
            error(err);
          });
      }, err => error(err));
    }
  },

  // gets an auth token
  token(code, redirectURI, success, error) {
    this.resolveReqServiceHost(authServiceName, host => {
      const params = {
        'code': code,
        'grant_type': 'authorization_code',
        'client_id': serverConfig.clientId(),
        'client_secret': serverConfig.clientSecret(),
        'redirect_uri': encodeURIComponent(redirectURI)
      };

      const body = Object.keys(params).map((key) => {
        return key + '=' + params[key];
      }).join('&');

      const config = {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      };

      const authServiceUrl = `http://${host.name}:${host.port}/v1/tenant/deluxe/auth/token`;
      axios.post(authServiceUrl, body, config)
        .then(response => {
          success(response.data);
        }).catch(err => {
        error(err);
      });
    }, err => error(err));
  },

  logout(refreshToken, success, error) {
    this.resolveReqServiceHost(authServiceName, host => {
      let params = {
        'refresh_token': refreshToken,
        'client_id': client_id,
        'client_secret': client_secret
      };

      const body = Object.keys(params).map((key) => {
        return encodeURIComponent(key) + '=' + encodeURIComponent(params[key]);
      }).join('&');

      let config = {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      };

      var authServiceUrl = `http://${host.name}:${host.port}/v1/tenant/deluxe/auth/logout`;
      axios.post(authServiceUrl, body, config)
        .then(response => {
          success(response.data);
        }).catch(err => {
        error(err);
      });
    }, err => error(err));
  },

  refresh(refreshToken, success, error) {
    this.resolveReqServiceHost(authServiceName, host => {
      let params = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refreshToken
      };

      const body = Object.keys(params).map((key) => {
        return encodeURIComponent(key) + '=' + encodeURIComponent(params[key]);
      }).join('&');

      let config = {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      };

      var authServiceUrl = `http://${host.name}:${host.port}/v1/tenant/deluxe/auth/token`;
      axios.post(authServiceUrl, body, config)
        .then(response => {
          success(response.data);
        }).catch(err => {
        error(err);
      });
    }, err => error(err));
  },

  user(token, success, error) {
    var tokenPayload = token.split('.')[1];
    var payload = JSON.parse(KJUR.b64toutf8(tokenPayload));
    var client_access = payload["resource_access"]["deluxe.conductor-ui"];
    var result = {
      name: payload["name"],
      preferred_username: payload["preferred_username"],
      email: payload["email"],
      expiration: new Date(), //token.ValidTo
      roles: client_access ? client_access.roles : null
    };
    success(result);
  }
};

export default AuthClient;
