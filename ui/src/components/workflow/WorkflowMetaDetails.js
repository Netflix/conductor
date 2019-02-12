import React, { Component } from 'react';
import { Tabs, Tab } from 'react-bootstrap';
import { connect } from 'react-redux';
import { getWorkflowMetaDetails, setWorkflowMetaDetails } from '../../actions/WorkflowActions';
import WorkflowMetaDia from './WorkflowMetaDia';
import WorkflowMetaInput from './WorkflowMetaInput';
import { getInputs, getDetails } from '../../actions/JSONActions';
import unescapeJs from 'unescape-js';

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
            <div><JSONTab wfs={JSON.stringify(this.state.workflowMeta, null, 2)} name={this.state.name} version={this.state.version} /></div>
          </Tab>
          <Tab eventKey={3} title="Input">
          <div><WorkflowMetaInput workflowForm={this.state.workflowForm} name={this.state.name}/></div>
          </Tab>
        </Tabs>

      </div>
    );
  }
};

class JSONTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      editingJSON: false,
      wfs: this.props.wfs,
      isNotParsable: false
    }
    this.editJSONswitch = this.editJSONswitch.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      wfs: nextProps.wfs
    })
  }

  editJSONswitch(e, which) {
    this.state.isNotParsable = false;
    if(which == 1) {
      if(this.state.editingJSON) {
        this.setState({wfs: this.editor.innerText});
        try {
          JSON.parse(unescapeJs(this.editor.innerText)).tasks;
        } catch(e) {
          this.setState({isNotParsable : true});
        }
        let toBeSent = JSON.parse(unescapeJs(this.editor.innerText)).tasks;
        console.log(setWorkflowMetaDetails(this.props.name, this.props.version, toBeSent));
      } else {
        this.editor.focus();
      }
    } else {
      this.editor.innerText = this.state.wfs;
    }
    this.setState({
      editingJSON: !this.state.editingJSON
    });
    this.forceUpdate();
  }

  render() {
    return(
        <div>
          <button className="btn btn-primary" onClick={(e) => this.editJSONswitch(e, 1)} style={{marginTop:'5px', marginBottom: '5px'}} >{this.state.editingJSON ? 'Save' : 'Edit'}</button>
          <button className="btn btn-default" onClick={(e) => this.editJSONswitch(e, 2)} style={{marginTop:'5px', marginBottom: '5px', marginLeft: '5px', display: this.state.editingJSON ? 'inline-block' : 'none'}} >Cancel</button>
          <div style={{marginTop: '10px', display: this.state.isNotParsable ? "block" : "none"}} className="alert alert-warning" role="alert">{this.state.isNotParsable ? "Could not parse JSON. Is the syntax correct?" : ""}</div>
          <pre ref={elem => this.editor = elem} className={this.state.editingJSON ? 'editingPre' : ''} contentEditable={this.state.editingJSON}>
            {this.state.wfs}
          </pre>
        </div>
    
    )
  };
};

export default connect(state => state.workflow)(WorkflowMetaDetails);