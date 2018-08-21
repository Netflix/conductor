/* eslint-disable no-undef */
import AuthFilter from '../../server/middleware/filters/authFilter';

test('Filters Pre Middleware', () => {
  const authFilter = new AuthFilter();
  const middleware = [];

  // Create Mock App
  const app = {
    use: f => {
      middleware.push(f);
    }
  };

  // Add the middleware
  authFilter.init(app);

  // Create the mock request
  const req = {
    headers: {
      authorization: 'Bearer header.body.signature'
    }
  };

  const res = {};

  // Execute the auth middleware
  middleware[0](req, res, () => {
    expect(req.headers.authorization).toBe(req.token);
  });
});

test('Filters Should bypass add auth token if auth header not present and call next', () => {
  const authFilter = new AuthFilter();
  const middleware = [];

  // Create Mock App
  const app = {
    use: f => {
      middleware.push(f);
    }
  };

  // Add the middleware
  authFilter.init(app);

  // Create the mock request
  const req = {
    headers: {
      foo: 'bar'
    }
  };

  const res = {};

  // Execute the auth middleware
  middleware[0](req, res, () => {
    expect(!req.headers.authorization).toBeTruthy();
    expect(!req.token).toBeTruthy();
  });
});
