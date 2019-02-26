import React, { Component } from 'react';
import { Tabs, Tab } from 'react-bootstrap';
import { connect } from 'react-redux';
import { getWorkflowMetaDetails, updateWorkflow } from '../../actions/WorkflowActions';
import WorkflowMetaDia from './WorkflowMetaDia';
import WorkflowMetaInput from './WorkflowMetaInput';
import { getInputs, getDetails } from '../../actions/JSONActions';
import JSONTab from './JSONTab';

class WorkflowMetaDetails extends Component {

  constructor(props) {
    super(props);
    this.state = {
      name : props.params.name,
      version : props.params.version,
      workflowMeta: {tasks: []},
      workflowForm: {
          labels: [],
          descs: [],
          values: []
      }
    };
  }

  componentWillReceiveProps(nextProps) {
    this.state.name = nextProps.params.name;
    this.state.version = nextProps.params.version;
    this.state.workflowMeta = nextProps.meta;
    this.getWorkflowInputDetails();
  }

  componentWillMount(){
    this.props.dispatch(getWorkflowMetaDetails(this.state.name, this.state.version));
  }

  getWorkflowInputDetails() {
      this.setState({
          workflowForm: {
              labels: getInputs(this.state.workflowMeta),
          }
      }, () => {
          this.setState({
              workflowForm: {
                  labels: this.state.workflowForm.labels,
                  ...getDetails(this.state.workflowMeta, this.state.workflowForm.labels)
              }
          })
      });
  }

  render() {
    let wf = this.state.workflowMeta;
    if(wf == null) {
      wf = {tasks: []};
    }

    return (
      <div className="ui-content">
        <Tabs>
          <Tab eventKey={1} title="Diagram">
            <div><WorkflowMetaDia meta={wf} tasks={[]}/></div>
          </Tab>
          <Tab ref={this.JSONTab} eventKey={2} title="JSON">
            <div><JSONTab wfs={this.state.workflowMeta} name={this.state.name} version={this.state.version} /></div>
          </Tab>
          <Tab eventKey={3} title="Input">
          <div><WorkflowMetaInput workflowForm={this.state.workflowForm} name={this.state.name}/></div>
          </Tab>
        </Tabs>

      </div>
    );
  }
};

export default connect(state => state.workflow)(WorkflowMetaDetails);