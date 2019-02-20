import request from 'superagent';
import dns from 'dns';

const authUrl = process.env.conductor_auth_url;
const authService = process.env.conductor_auth_service;
const authEndpoint = process.env.conductor_auth_endpoint;
const clientId = process.env.conductor_auth_clientId;
const clientSecret = process.env.conductor_auth_clientSecret;
const authEnabled = (authUrl || authService) && authEndpoint && clientId && clientSecret;

function getAuthUrl() {
  return new Promise((resolve) => {
    if (!authService) {
      console.log('conductor_auth_service is not defined. Falling back to ' + authUrl);
      resolve(authUrl);
      return;
    }

    dns.resolveSrv(authService, function (err, addresses) {
      if (addresses && addresses.length > 0) {
        let instance = addresses[0];
        resolve('http://' + instance.name + ':' + instance.port + authEndpoint);
      } else {
        console.log('Service lookup failed for ' + authService + '. Falling back to ' + authUrl);
        resolve(authUrl);
      }
    });
  })
}

const AuthManager = {

  getAuthToken: () => new Promise((resolve, reject) => {
    if (!authEnabled) {
      console.log('Auth is not enabled');
      resolve(null);
      return;
    }

    getAuthUrl().then(url => {
      console.log('getAuthUrl resulted in ' + url);

      request
        .post(url)
        .send('grant_type=client_credentials')
        .send('client_id=' + clientId)
        .send('client_secret=' + clientSecret)
        .end((err, res) => {
          if (err) {
            console.log('getAuthToken failed', err);
            reject(err);
          } else {
            resolve(res.body.access_token);
          }
        });
    });
  })
};

export default AuthManager;