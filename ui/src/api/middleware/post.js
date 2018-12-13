export default class PostMiddleware {
  init(app) {
    // placeholder for post routes middleware.
    app.use(function(err, req, res, next) {
      app.log.error(err);
      res.status(err.status || 500);
      if (err.response) {
        res.status(err.response.status || 500);
        if (err.response.text) {
          res.send(err.response.text);
        } else {
          next(err);
        }
      } else if (err.message){
        send(err.message);
      } else {
        next(err);
      }
    });
  }
}