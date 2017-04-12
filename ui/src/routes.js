import App from './components/App';
import Workflow from './components/workflow/executions/WorkflowList';
import Event from './components/event/EventList';
import EventExecs from './components/event/EventExecs';
import WorkflowDetails from './components/workflow/executions/WorkflowDetails';
import WorkflowDia from './components/workflow/executions/WorkflowDia';
import WorkflowMetaList from './components/workflow/WorkflowMetaList';
import TasksMetaList from './components/workflow/tasks/TasksMetaList';
import WorkflowMetaDetails from './components/workflow/WorkflowMetaDetails';
import WorkflowMetaDia from './components/workflow/WorkflowMetaDia';
import Intro from './components/common/Home';
import Help from './components/common/Help';

const routeConfig = [
  { path: '/',
    component: App,
    indexRoute: { component: Intro },
    childRoutes: [
      { path: 'workflow/metadata', component: WorkflowMetaList },
      { path: 'workflow/metadata/:name/:version', component: WorkflowMetaDetails },
      { path: 'workflow/metadata/tasks', component: TasksMetaList },
      { path: 'workflow', component: Workflow },
      { path: 'workflow/id/:workflowId', component: WorkflowDetails },
      { path: 'events', component: Event },
      { path: 'events/executions', component: EventExecs },      
      { path: 'help', component: Help }
    ]
  }
]

export default routeConfig;
