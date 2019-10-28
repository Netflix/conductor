import {Router} from 'express';
import log4js from 'log4js';
import authClient from '../core/AuthClient';
import DnsResolver from '../core/DnsResolver';
import {serverConfig} from '../core/ServerConfig';
import axios from "axios";

const logger = log4js.getLogger('server.routes.auth');

const router = new Router();

router.post('/auth/login', (req, res) => {
  authClient.login(req.body.redirectURI, data => {
    res.send({url: data});
  }, error => {
    logger.error(`in route /auth/login error: ${error}`);
    res.status(error.response.status).send(error.response.data);
  });
});

router.post('/auth/token', (req, res) => {
  authClient.token(req.body.code, req.body.redirectURI, data => {
    res.send(data);
  }, error => {
    logger.error(`in route /auth/token error: ${error}:${JSON.stringify(error.response.data)}`);
    res.status(error.response.status).send(error.response.data);
  });
});

router.post('/auth/logout', (req, res) => {
  authClient.logout(req.body.refresh_token, data => {
    res.send(data);
  }, error => {
    logger.error(`in route /auth/logout error: ${error}`);
    res.status(error.response.status).send(error.response.data);
  });
});

router.post('/auth/refresh', (req, res) => {
  authClient.refresh(req.body.refresh_token, data => {
    res.send(data);
  }, error => {
    logger.error(`in route /auth/refresh error: ${error}`);
    res.status(error.response.status).send(error.response.data);
  });
});

router.post('/auth/user', (req, res) => {
  authClient.user(req.body.accessToken, data => {
    res.json(data);
  }, error => {
    logger.error(`in route /auth/user error: ${error}`);
    res.status(error.response.status).send(error.response.data);
  });
});

router.get('/auth/testcors', (req, res) => {
  res.json(authClient.testcors());
});

module.exports = router;