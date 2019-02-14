import {Router} from 'express';
import http from '../core/HttpClient';
import moment from 'moment';
import filter from "lodash/fp/filter";
import forEach from "lodash/fp/forEach";
import map from "lodash/fp/map";
import transform from "lodash/transform";
import identity from "lodash/identity";
import bodyParser from "body-parser";

const router = new Router();
const baseURL = process.env.WF_SERVER;
const baseURL2 = baseURL + 'workflow/';
const baseURL2ByTasks = baseURL2 + 'search-by-task';
const baseURLMeta = baseURL + 'metadata/';
const baseURLTask = baseURL + 'tasks/';
const cronJobs = [];
const cronHistory = [];

var CronJob = require('cron').CronJob
router.use(bodyParser.urlencoded({ extended: true}));
router.use(bodyParser.json());


router.get('/', async (req, res, next) => {
  try {

    let freeText = [];
    if(req.query.freeText != '') {
      freeText.push(req.query.freeText);
    }else {
      freeText.push('*');
    }

    let h = '-1';
    if(req.query.h != 'undefined' && req.query.h != ''){
      h = req.query.h;
    }
    if(h != '-1'){
      freeText.push('startTime:[now-' + h + 'h TO now]');
    }
    let start = 0;
    if(!isNaN(req.query.start)){
      start = req.query.start;
    }

    let query = req.query.q;
    const url = baseURL2 + 'search?size=100&sort=startTime:DESC&freeText=' + freeText.join(' AND ') + '&start=' + start + '&query=' + query;
    const result = await http.get(url);
    const hits = result.results;
    res.status(200).send({result: {hits:hits, totalHits: result.totalHits}});
  } catch (err) {
    next(err);
  }
});

const LOG_DATE_FORMAT = 'MM/DD/YY, HH:mm:ss:SSS';

router.get('/search-by-task/:taskId', async (req, res, next) => {
  try {

    let freeText = [];
    if(req.query.freeText != '') {
      freeText.push(req.params.taskId);
    }else {
      freeText.push('*');
    }

    let h = '-1';
    if(req.query.h !== undefined && req.query.h != 'undefined' && req.query.h != ''){
      h = req.query.h;
    }
    if(h != '-1'){
      freeText.push('startTime:[now-' + h + 'h TO now]');
    }
    let start = 0;
    if(!isNaN(req.query.start)){
      start = req.query.start;
    }

    let query = req.query.q || "";
    const url = baseURL2 + 'search-by-tasks?size=100&sort=startTime:DESC&freeText=' + freeText.join(' AND ') + '&start=' + start;
    const result = await http.get(url);
    const hits = result.results;
    res.status(200).send({result: {hits:hits, totalHits: result.totalHits}});
  } catch (err) {
    next(err);
  }
})

router.get('/id/:workflowId', async (req, res, next) => {
    try {
        const result = await http.get(baseURL2 + req.params.workflowId + '?includeTasks=true');
        const meta = await http.get(baseURLMeta + 'workflow/' + result.workflowType + '?version=' + result.version);

        const subs = filter(identity)(map(task => {
            if (task.taskType === 'SUB_WORKFLOW') {
                const subWorkflowId = task.inputData && task.inputData.subWorkflowId;

                if (subWorkflowId != null) {
                    return {
                        name: task.inputData.subWorkflowName,
                        version: task.inputData.subWorkflowVersion,
                        referenceTaskName: task.referenceTaskName,
                        subWorkflowId: subWorkflowId
                    };
                }
            }
        })(result.tasks));

        result.tasks.forEach(task => {
            if (task.taskType === 'SUB_WORKFLOW') {
                const subWorkflowId = task.inputData && task.inputData.subWorkflowId;

                if (subWorkflowId != null) {
                    subs.push({
                        name: task.inputData.subWorkflowName,
                        version: task.inputData.subWorkflowVersion,
                        referenceTaskName: task.referenceTaskName,
                        subWorkflowId: subWorkflowId
                    });
                }
            }
        });

        const logs = map(task => Promise.all([task, http.get(baseURLTask + task.taskId + '/log')]))(result.tasks);

        await Promise.all(logs).then(result => {
            forEach(([task, logs]) => {
                if (logs) {
                    task.logs = map(({createdTime, log}) => `${moment(createdTime).format(LOG_DATE_FORMAT)} : ${log}`)(logs)
                }
            })(result);
        });

        const promises = map(({name, version, subWorkflowId, referenceTaskName}) => Promise.all([
            referenceTaskName,
            http.get(baseURLMeta + 'workflow/' + name + '?version=' + version),
            http.get(baseURL2 + subWorkflowId + '?includeTasks=true')
        ]))(subs);

        const subworkflows = await Promise.all(promises).then(result => {
            return transform(result, (result, [key, meta, wfe]) => {
                result[key] = {meta, wfe};
            }, {});
        });

        res.status(200).send({result, meta, subworkflows: subworkflows});
    } catch (err) {
        next(err);
    }
});
router.delete('/terminate/:workflowId', async (req, res, next) => {
  try {
    const result = await http.delete(baseURL2 + req.params.workflowId);
    res.status(200).send({result: req.params.workflowId });
  } catch (err) {
    next(err);
  }
});
router.delete('/workflow/:workflowId', async (req, res, next) => {
  try {
    const result = await http.delete(baseURL2 + req.params.workflowId + '/remove');
    res.status(200).send({result: req.params.workflowId });
  } catch (err) {
    next(err);
  }
});

router.post('/restart/:workflowId', async (req, res, next) => {
  try {
    const result = await http.post(baseURL2 + req.params.workflowId + '/restart');
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});

//CronJobs requests
router.post('/workflow/:workflowName', async (req, res, next) => {
  if (!req.body.cronExp) {
    const result = await http.post(baseURL2 + req.params.workflowName, JSON.stringify(req.body.json));
    res.status(200).send(result);
  } else {
    let name = req.params.workflowName;
    let desc = req.body.cronDesc;
    let id = Math.floor((1 + Math.random()) * 0x10000).toString();
    let job = new CronJob(req.body.cronExp, async function() {
        console.log('Workflow job ID: %s executed', id);

        const result = await http.post(baseURL2 + req.params.workflowName, JSON.stringify(req.body.json));

        let history = {
            id: id,
            wfid: result.text
        };

        cronHistory.push(history);

    }, null, true);


    let jobData = {
      id: id,
      name: name,
      desc: desc,
      ...job
    };
    cronJobs.push(jobData);
    res.status(200).send("Workflow successfully scheduled.");

  }
});

router.get('/crondata', async (req, res, next) => {
    try {
        let cacheH = [];
        let cacheS = [];
        let history;
        let scheduledJobs;

        history = JSON.stringify(cronHistory, function (key, value) {
            if (typeof value === 'object' && value !== null) {
                if (cacheH.indexOf(value) !== -1) {
                    return;
                }
                cacheH.push(value);
            }
            return value;
        });
        cacheH = null;

        scheduledJobs = JSON.stringify(cronJobs, function (key, value) {
            if (typeof value === 'object' && value !== null) {
                if (cacheS.indexOf(value) !== -1) {
                    return;
                }
                cacheS.push(value);
            }
            return value;
        });
        cacheS = null;


        res.status(200).send({history: history, scheduledJobs: scheduledJobs});
    } catch (err) {
        next(err);
    }
});

router.post('/cronjobs/stop/:cronjobId', async (req, res, next) => {
  try {
    let index = cronJobs.findIndex(obj => obj.id == req.params.cronjobId);
    cronJobs[index].context.stop();
    console.log("Workflow job ID: %s stopped", req.params.cronjobId);
    res.status(200).send("Workflow stopped successfully!");
  } catch (err) {
    next(err);
  }
});

router.post('/cronjobs/start/:cronjobId', async (req, res, next) => {
  try {
    let index = cronJobs.findIndex(obj => obj.id == req.params.cronjobId);
    cronJobs[index].context.start();
    console.log("Workflow job ID: %s resumed", req.params.cronjobId);
    res.status(200).send("Workflow resumed successfully!");
  } catch (err) {
    next(err);
  }
});

router.delete('/cronjobs/delete/:cronjobId', async (req, res, next) => {
  try {
    let index = cronJobs.findIndex(obj => obj.id == req.params.cronjobId);
    cronJobs.splice(index, 1);
    console.log("Workflow job ID: %s deleted", req.params.cronjobId);
    res.status(200).send("Workflow deleted successfully!");
  } catch (err) {
    next(err);
  }
});
//////////////////////////
router.post('/retry/:workflowId', async (req, res, next) => {
  try {
    const result = await http.post(baseURL2 + req.params.workflowId + '/retry');
    res.status(200).send({result: req.params.workflowId });
  } catch (err) {
    next(err);
  }
});

router.post('/pause/:workflowId', async (req, res, next) => {
  try {
    const result = await http.put(baseURL2 + req.params.workflowId + '/pause');
    res.status(200).send({result: req.params.workflowId });
  } catch (err) {
    next(err);
  }
});

router.post('/resume/:workflowId', async (req, res, next) => {
  try {
    const result = await http.put(baseURL2 + req.params.workflowId + '/resume');
    res.status(200).send({result: req.params.workflowId });
  } catch (err) {
    next(err);
  }
});

//metadata
router.get('/metadata/workflow/:name/:version', async (req, res, next) => {
  try {
    const result = await http.get(baseURLMeta + 'workflow/' + req.params.name + '?version=' + req.params.version);
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});
router.get('/metadata/workflow', async (req, res, next) => {
  try {
    const result = await http.get(baseURLMeta + 'workflow');
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});
router.get('/metadata/taskdef', async (req, res, next) => {
  try {
    const result = await http.get(baseURLMeta + 'taskdefs');
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});
router.get('/task/log/:taskId', async (req, res, next) => {
  try {
    const logs = await http.get(baseURLTask + req.params.taskId + '/log');
    res.status(200).send({logs});
  } catch (err) {
    next(err);
  }
});
router.get('/queue/data', async (req, res, next) => {
  try {
    const sizes = await http.get(baseURLTask + 'queue/all');
    const polldata = await http.get(baseURLTask + 'queue/polldata/all');
    polldata.forEach(pd=>{
      var qname = pd.queueName;

      if(pd.domain != null){
        qname = pd.domain + ":" + qname;
      }
      pd.qsize = sizes[qname];
    });
    res.status(200).send({polldata});
  } catch (err) {
    next(err);
  }
});

//metadata post
router.put('/metadata', async (req, res, next) => {
  try {
    let workflowDesc = req.body;
    const response = await http.put(baseURLMeta + 'workflow/', workflowDesc);
    res.status(204);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
