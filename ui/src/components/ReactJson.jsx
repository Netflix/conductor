import React, { useRef } from "react";
import Editor from "@monaco-editor/react";
import { makeStyles } from "@material-ui/styles";
import { InputLabel, IconButton, Tooltip } from "@material-ui/core";
import clsx from "clsx";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ExpandLessIcon from "@material-ui/icons/ExpandLess";
import FileCopyIcon from "@material-ui/icons/FileCopy";

const useStyles = makeStyles({
  monaco: {},
  outerWrapper: {
    height: "100%",
    display: "flex",
    flexDirection: "column",
  },
  editorWrapper: {
    flex: "1",
    marginLeft: 10,
  },
  label: {
    marginTop: 5,
    marginBottom: 10,
    flex: 1,
  },
  toolbar: {
    paddingRight: 15,
    paddingLeft: 15,
    display: "flex",
    alignItems: "flex-start",
    flexDirection: "row",
  },
});

export default function ReactJson({ className, label, src }) {
  const classes = useStyles();
  const editorRef = useRef(null);

  function handleEditorMount(editor) {
    editorRef.current = editor;
  }

  function handleCopyAll() {
    const editor = editorRef.current;
    const range = editor.getModel().getFullModelRange();
    editor.setSelection(range);
    editor
      .getAction("editor.action.clipboardCopyWithSyntaxHighlightingAction")
      .run();
  }

  function handleExpandAll() {
    editorRef.current.getAction("editor.unfoldAll").run();
  }

  function handleCollapse() {
    editorRef.current.getAction("editor.foldLevel2").run();
  }

  return (
    <div className={clsx([classes.outerWrapper, className])}>
      <div className={classes.toolbar}>
        {label && (
          <InputLabel variant="outlined" className={classes.label}>
            {label}
          </InputLabel>
        )}
        <Tooltip title="Collapse All">
          <IconButton size="small" onClick={handleCollapse}>
            <ExpandLessIcon />
          </IconButton>
        </Tooltip>
        <Tooltip title="Expand All">
          <IconButton size="small" onClick={handleExpandAll}>
            <ExpandMoreIcon />
          </IconButton>
        </Tooltip>
        <Tooltip title="Copy All">
          <IconButton size="small" onClick={handleCopyAll}>
            <FileCopyIcon />
          </IconButton>
        </Tooltip>
      </div>
      <div className={classes.editorWrapper}>
        <Editor
          className={classes.monaco}
          height="100%"
          defaultLanguage="json"
          onMount={handleEditorMount}
          defaultValue={JSON.stringify(src, null, 2)}
          options={{
            readOnly: true,
            tabSize: 2,
            minimap: { enabled: false },
            lightbulb: { enabled: false },
            scrollbar: { useShadows: false },
            quickSuggestions: false,
            showFoldingControls: "always",
            lineNumbers: "off",

            // Undocumented see https://github.com/Microsoft/vscode/issues/30795#issuecomment-410998882
            lineDecorationsWidth: 0,
            lineNumbersMinChars: 0,
            renderLineHighlight: "none",

            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            overviewRulerBorder: false,
          }}
        />
      </div>
    </div>
  );
}
