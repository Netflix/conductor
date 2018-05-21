import {Router} from 'express';

const router = new Router();

const TLD = process.env.TLD;
const WF_SERVER = process.env.WF_SERVER;
const WF_SERVICE = process.env.WF_SERVICE;
const APP_VERSION = process.env.APP_VERSION || "1.7.6-deluxe-ui";
const AUTH = process.env.conductor_auth_url;

router.get('/status', async (req, res, next) => {
  try {
    res.status(200).send({version:APP_VERSION});
  } catch(err) {
    next(err);
  }
});

router.get('/dependencies', async (req, res, next) => {
  try {
    let conductor = WF_SERVICE || WF_SERVER;
    let vault = "vault.service." + TLD;

    let endpoints = [conductor, vault];
    if (AUTH) {
      endpoints.push(AUTH);
    }
    let dependencies = [
      {name: "conductor-server", version: "v1", scheme: "http", external: false},
      {name: "vault", version: "v1", scheme: "http", external: false},
    ];
    if (AUTH) {
      dependencies.push({name: "auth", version: "v1", scheme: "https", external: false});
    }

    res.status(200).send({version: APP_VERSION, endpoints, dependencies});
  } catch(err) {
    next(err);
  }
});

module.exports = router;
