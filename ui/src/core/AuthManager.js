import request from 'superagent';

const authUrl = process.env.conductor_auth_url;
const clientId = process.env.conductor_auth_clientId;
const clientSecret = process.env.conductor_auth_clientSecret;
const authEnabled = authUrl !== undefined && clientId !== undefined && clientSecret !== undefined &&
  authUrl !== '' && clientId !== '' && clientSecret !== '';

const AuthManager = {
  isAuthEnabled: () => {
    return authEnabled;
  },

  getAuthToken: () => new Promise((resolve, reject) => {
    if (!authEnabled) {
      resolve(null);
      return;
    }

    request
      .post(authUrl)
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
  }),
};

export default AuthManager;