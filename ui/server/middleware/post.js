/* eslint-disable class-methods-use-this */
const ErrorFilter = require('./filters/errorFilter');

class PostMiddleware {
  init(app) {
    new ErrorFilter().init(app);
  }
}

module.exports = PostMiddleware;
