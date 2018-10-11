export function getInputs(workflowMeta) {

    var matchArray = [];
    var rgxInput = /\workflow.input([\w.])+\}/igm
    var rgxTrim = /[^\.]+(?=\})/igm

    if (workflowMeta) {
      var json = JSON.stringify(workflowMeta, null, 2);
      matchArray = json.match(rgxInput);
    }
    if (matchArray) {
      var matchString = matchArray.join();
      var sortedArray = matchString.match(rgxTrim);
      var inputsArray = _.uniq(sortedArray);
    } else {
      inputsArray = [];
    }

    return inputsArray;

  }

  export function getDetails(workflowMeta, inputsArray) {

    var inputsArray = inputsArray;
    var json = JSON.stringify(workflowMeta, null, 2);
    var detailsArray = [];
    var tmpDesc = [];
    var tmpValue = [];
    var descs = [];
    var values = [];
    var regex1 = /\[.*?\[/;
    var regex2 = /\].*?\]/;
    var regex3 = /[^[\]"]+/;
    var regex4 = /\[(.*?)\]/;

    if (inputsArray[0] != "") {
      for (let i = 0; i < inputsArray.length; i++) {
        var RegExp3 = new RegExp("\\b" + inputsArray[i] + "\\[.*?\\" + "\]" + "\"", 'igm');
        detailsArray[i] = json.match(RegExp3);
      }
    }

    for (let i = 0; i < detailsArray.length; i++) {
      if (detailsArray[i]) {

        tmpDesc[i] = detailsArray[i][0].match(regex1);
        tmpValue[i] = detailsArray[i][0].match(regex2);

        if (tmpDesc[i] == null) {
          tmpDesc[i] = detailsArray[i][0].match(regex4);
          
          descs[i] = tmpDesc[i][1];
          values[i] = null;

        } else {
          tmpDesc[i] = tmpDesc[i][0].match(regex3);
          tmpValue[i] = tmpValue[i][0].match(regex3);

          descs[i] = tmpDesc[i][0];
          values[i] = tmpValue[i][0];
        }

      } else {
        descs[i] = null;
        values[i] = null;
      }
    }

    return { descs, values };

    }
