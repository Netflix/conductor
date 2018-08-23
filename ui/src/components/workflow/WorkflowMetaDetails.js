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
      desc: [],
      value: []
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

  getDetails() {

    var inputsArray = this.state.inputsArray;
    var json = JSON.stringify(this.state.workflowMeta, null, 2);
    var detailsArray = [];
    var tmpDesc = [];
    var tmpValue = [];
    var desc = [];
    var value = [];
    var regex1 = /\[.*?\[/;
    var regex2 = /\].*?\]/;
    var regex3 = /[^[\]"]+/;

    if (inputsArray[0] != "") {
      for (let i = 0; i < inputsArray.length; i++) {
        var RegExp3 = new RegExp("\\b" + inputsArray[i] + ".*?\\" + "\]" + "\"", 'igm');
        detailsArray[i] = json.match(RegExp3);
      }
    }

    for (let i = 0; i < detailsArray.length; i++) {
      if (detailsArray[i]) {

        tmpDesc[i] = detailsArray[i][0].match(regex1);
        tmpValue[i] = detailsArray[i][0].match(regex2);

        tmpDesc[i] = tmpDesc[i][0].match(regex3);
        tmpValue[i] = tmpValue[i][0].match(regex3);

        desc[i] = tmpDesc[i][0];
        value[i] = tmpValue[i][0];
      } else {
        desc[i] = null;
        value[i] = null;
      }
    }

    this.state.desc = desc;
    this.state.value = value;

  }

  getInputs() {

    var matchArray = [];
    var rgxInput = /\input([\w.])+\}/igm
    var rgxTrim = /[^\.]+(?=\})/igm

    if (this.state.workflowMeta) {
      var json = JSON.stringify(this.state.workflowMeta, null, 2);
      matchArray = json.match(rgxInput);
    }
    if (matchArray) {
      var matchString = matchArray.join();
      var sortedArray = matchString.match(rgxTrim);
      var inputsArray = _.uniq(sortedArray);
      this.state.inputsArray = inputsArray;
    } else {
      this.state.inputsArray = [];
    }

  }

  updateState() {
    this.getInputs();
    this.getDetails();
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
