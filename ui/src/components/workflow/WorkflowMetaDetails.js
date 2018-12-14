import React from 'react';
import { Tabs, Tab } from 'react-bootstrap';
import { connect } from 'react-redux';
import { getWorkflowMetaDetails } from '../../actions/WorkflowActions';

import WorkflowMetaDia from './WorkflowMetaDia'
import WorkflowForm from './WorkflowCreation'

class WorkflowMetaDetails extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      name : props.params.name,
      version : props.params.version,
      workflowMeta: {tasks: []}
    };
  }

  componentWillReceiveProps(nextProps) {
    this.state.name = nextProps.params.name;
    this.state.version = nextProps.params.version;
    this.state.workflowMeta = nextProps.meta;
  }

  componentWillMount(){
    this.props.dispatch(getWorkflowMetaDetails(this.state.name, this.state.version));
  }

  render() {
    let wf = this.state.workflowMeta;
    let name = this.state.name;
    let version = this.state.version;

    if(wf == null) {
      wf = {tasks: []};
    }
    return (
      <div className="ui-content" style={{marginTop: "5 em"}}>
        <Tabs>
          <Tab eventKey={1} title="Diagram">
            <div><WorkflowMetaDia meta={wf} tasks={[]}/></div>
          </Tab>
          <Tab eventKey={2} title="JSON">
            <div><pre>
              {JSON.stringify(this.state.workflowMeta, null, 2)}
          </pre></div>
          </Tab>
          <Tab eventKey={3} title="Create">
            <div style={{margin: "30 em"}}>
              <WorkflowForm workflowDef = {wf} name = {name} version = {version}/>
            </div>
          </Tab>
        </Tabs>
      </div>
    );
  }
}

export default connect(state => state.workflow)(WorkflowMetaDetails);