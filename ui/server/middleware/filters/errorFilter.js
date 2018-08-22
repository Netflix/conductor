/* eslint-disable consistent-return,class-methods-use-this,no-unused-vars,no-console */

class ErrorFilter {
  init(app) {
    app.use(({ stack, message }, req, res, next) => {
      // console.log('Error ee: ', err);
      // console.log('Error Stack: ', err.stack);
      // console.log('Error Stack: ', err.stackTrace);
      res.status(500).send({ stack, message });
    });
  }
}

module.exports = ErrorFilter;
