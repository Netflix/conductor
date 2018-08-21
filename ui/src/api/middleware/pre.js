/* eslint-disable class-methods-use-this */
const AuthFilter = require('./filters/authFilter');

class PreMiddleware {
  init(app) {
    new AuthFilter().init(app);
  }
}

module.exports = PreMiddleware;
