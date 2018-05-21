import {Router} from 'express';

const router = new Router();

const TLD = process.env.TLD;
const WF_SERVER = process.env.WF_SERVER;
const WF_SERVICE = process.env.WF_SERVICE;
const AUTH = process.env.conductor_auth_url;

router.get('/status', async (req, res, next) => {
  try {
    res.status(200).send({version: "1.7.6-SNAPSHOT"});
  } catch(err) {
    next(err);
  }
});

router.get('/dependencies', async (req, res, next) => {
  try {
    let conductor = WF_SERVICE || WF_SERVER;
    let vault = "vault.service." + TLD;

    let version = "1.7.6-SNAPSHOT";
    let endpoints = [conductor, vault];
    if (AUTH) {
      endpoints.push(AUTH);
    }
    let dependencies = [
      {name: "conductor-server", version: "v1", scheme: "https", external: false},
      {name: "vault", version: "v1", scheme: "http", external: false},
    ];
    if (AUTH) {
      dependencies.push({name: "auth", version: "v1", scheme: "https", external: false});
    }

    res.status(200).send({version, endpoints, dependencies});
  } catch(err) {
    next(err);
  }
});

module.exports = router;
