import React, { Component } from 'react';
import { updateWorkflow } from '../../actions/WorkflowActions';
import { connect } from 'react-redux';
import unescapeJs from 'unescape-js';

class JSONTab extends Component {
    constructor(props) {
      super(props);
      this.state = {
        editingJSON: false,
        wfs: this.props.wfs,
        isNotParsable: false,
        reloading: false
      }
      this.editJSONswitch = this.editJSONswitch.bind(this);
    }
  
    componentWillReceiveProps(nextProps) {
      this.setState({
        wfs: nextProps.wfs
      })
    }
  
    editJSONswitch(e, which) {
      let parseErr = null;
      this.state.isNotParsable = false;
      if(which == 1) {
        if(this.state.editingJSON) {
          try {
            JSON.parse(this.editor.innerText);
          } catch(e) {
            parseErr = e;
          }
          if(parseErr == null) {
            this.setState({wfs: this.editor.innerText});
            let toBeSent = JSON.parse("["+this.editor.innerText+"]");
            
            this.props.dispatch(updateWorkflow(toBeSent));
            this.setState({
              reloading: true
            })
            location.reload();
          } else {
            this.setState({isNotParsable : true});
          }
        } else {
          this.editor.focus();
        }
      } else {
        this.editor.innerText = this.state.wfs;
      }
      if(parseErr == null) {
        this.setState({
            editingJSON: !this.state.editingJSON
        });
      }
      this.forceUpdate();
    }
  
    render() {
      return(
          <div>
            <button className="btn btn-default" onClick={(e) => this.editJSONswitch(e, 2)} style={{marginTop:'5px', marginBottom: '5px', display: this.state.editingJSON ? 'inline-block' : 'none'}} >Cancel</button>
            <button className="btn btn-primary" onClick={(e) => this.editJSONswitch(e, 1)} style={{marginTop:'5px', marginBottom: '5px', marginLeft: '5px'}} disabled={this.state.reloading}>{this.state.editingJSON ? 'Save' : 'Edit'}</button>
            <div style={{marginTop: '10px', display: this.state.isNotParsable ? "block" : "none"}} className="alert alert-warning" role="alert">{this.state.isNotParsable ? "Could not parse JSON. Is the syntax correct?" : ""}</div>
            <pre ref={elem => this.editor = elem} className={this.state.editingJSON ? 'editingPre' : ''} contentEditable={this.state.editingJSON}>
              {this.state.wfs}
            </pre>
          </div>
      
      )
    };
  };
  
  export default connect(state => state.workflow)(JSONTab);