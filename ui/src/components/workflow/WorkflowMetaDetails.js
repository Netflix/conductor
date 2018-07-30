import React, { Component } from 'react';
import { Link } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Grid, Row, Col, Well, OverlayTrigger,Button,Popover, Panel, Tabs, Tab } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import { connect } from 'react-redux';
import { getWorkflowMetaDetails } from '../../actions/WorkflowActions';
import WorkflowMetaDia from './WorkflowMetaDia';
import WorkflowMetaInput from './WorkflowMetaInput';

class WorkflowMetaDetails extends Component {

  constructor(props) {
    super(props);
    this.state = {
      name : props.params.name,
      version : props.params.version,
      workflowMeta: {tasks: []},
      inputsArray: [],
    };
  }

  componentWillReceiveProps(nextProps) {
    this.state.name = nextProps.params.name;
    this.state.version = nextProps.params.version;
    this.state.workflowMeta = nextProps.meta;
    this.updateState();
  }

  componentWillMount(){
    this.props.dispatch(getWorkflowMetaDetails(this.state.name, this.state.version)); 
  }


  updateState(){
   
    if(this.state.workflowMeta) {
      var jsonInput = JSON.stringify(this.state.workflowMeta, null, 2);
      var RegExp = /\input([\w.])+\}/igm
      var RegExp2 = /[^\.]+(?=\})/igm
      var matchArray = jsonInput.match(RegExp);
      }

  if(matchArray) {
      var matchString = matchArray.join();
      var sortedArray = matchString.match(RegExp2);
      var inputsArray = _.uniq(sortedArray);
      this.state.inputsArray = inputsArray;
      }

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
          <Tab eventKey={2} title="JSON">
            <div><pre>
              {JSON.stringify(this.state.workflowMeta, null, 2)};
          </pre></div>
          </Tab>
          <Tab eventKey={3} title="Input">
          <div><WorkflowMetaInput meta={this.state}/></div>
          </Tab>
        </Tabs>

      </div>
    );
  }
};
export default connect(state => state.workflow)(WorkflowMetaDetails);
