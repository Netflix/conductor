import React, { Component } from 'react';
import { updateWorkflow } from '../../actions/WorkflowActions';
import { connect } from 'react-redux';
import unescapeJs from 'unescape-js';
import UnescapeButton from '../common/UnescapeButton';
import Popup from '../common/Popup';

class JSONTab extends Component {
    constructor(props) {
      super(props);
      this.state = {
        editingJSON: false,
        wfs: this.convertToString(this.props.wfs),
        isNotParsable: false,
        reloading: false,
        showPopup: false,
        popUpElement: {}
      }
      this.editJSONswitch = this.editJSONswitch.bind(this);
    }
  
    componentWillReceiveProps(nextProps) {
      this.setState({
        wfs: this.convertToString(nextProps.wfs)
      })
    }

    convertToString(object) {
      object = this.updateObject(object, "\r\n")
      return JSON.stringify(object, null, 2); 
    }

    updateObject(object, search) {
      if (object) {
        Object.keys(object).forEach((k) => {
          if (object[k] && typeof object[k] === 'object') {
            return this.updateObject(object[k], search)
          }
          if (typeof object[k] === 'string') {
            if (object[k].includes(search)) {
              object[k] = "<span class='editable-string' title='Click to edit'>" + object[k] + '</span>';;
            }
          }
        });
      return object;
      }
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
          if(this.unescapeButton.state.isUnescaped) {
            this.unescapeButton.doUnescape();  
          }
          this.editor.focus();
        }
      } else {
        this.editor.innerHTML = this.state.wfs;
      }
      if(parseErr == null) {
        this.setState({
            editingJSON: !this.state.editingJSON
        });
      }
      this.forceUpdate();
    }

    onClick(e) {
      var element = e.target;
      if (element.className == 'editable-string') {
        this.openPopup(element);
      }
    }

    openPopup(element) {
      if (this.state.editingJSON) {
        this.setState({
          showPopup: true,
          popUpElement: element
        });
      }
    }

    closePopup() {
      this.setState({
        showPopup: false,
        popUpText: ''
      });
    }

    closeAndSavePopup(element, text) {
      element.textContent = text.textContent.replace(/"/g, '\\"').replace(/\n/g, "\\r\\n"); 
      this.setState({
        showPopup: false,
        popUpText: ''
      });
    }

    render() {
      return(
          <div>
            <button className="btn btn-default" onClick={(e) => this.editJSONswitch(e, 2)} style={{marginTop:'5px', marginBottom: '5px', display: this.state.editingJSON ? 'inline-block' : 'none'}} >Cancel</button>
            <button className="btn btn-primary" onClick={(e) => this.editJSONswitch(e, 1)} style={{marginTop:'5px', marginBottom: '5px', marginLeft: '5px'}} disabled={this.state.reloading}>{this.state.editingJSON ? 'Save' : 'Edit'}</button>
            <span style={{ display: this.state.editingJSON ? 'none' : 'inline-block' }}>
              <UnescapeButton target='jsonedit' medium='true' ref={instance => { this.unescapeButton = instance; }} />          
            </span>
            <div style={{marginTop: '10px', display: this.state.isNotParsable ? "block" : "none"}} className="alert alert-warning" role="alert">{this.state.isNotParsable ? "Could not parse JSON. Is the syntax correct?" : ""}</div>
              <pre id="jsonedit" ref={elem => this.editor = elem} 
                className={this.state.editingJSON ? 'editingPre' : ''} 
                contentEditable={this.state.editingJSON} 
                dangerouslySetInnerHTML= {{ __html: this.state.wfs }}
                onClick={this.onClick.bind(this)}>
              </pre>
            {this.state.showPopup ? <Popup element={this.state.popUpElement} closeAndSave={this.closeAndSavePopup.bind(this)} close={this.closePopup.bind(this)}/> : null }
          </div>
      
      )
    };
  };
  
  export default connect(state => state.workflow)(JSONTab);