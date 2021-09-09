import React from "react";
import ReactJson from "../../components/ReactJson";

export default function ExecutionJson({ execution }) {
  return <ReactJson src={execution} collapsed={1} />;
}
