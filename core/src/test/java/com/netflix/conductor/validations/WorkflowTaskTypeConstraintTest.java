package com.netflix.conductor.validations;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.tasks.Terminate;
import com.netflix.conductor.dao.MetadataDAO;
import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.cfg.ConstraintMapping;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.executable.ExecutableValidator;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class WorkflowTaskTypeConstraintTest {

    private static Validator validator;
    private MetadataDAO mockMetadataDao;
    private HibernateValidatorConfiguration config;

    @Before
    public void init() {
        ValidatorFactory vf = Validation.buildDefaultValidatorFactory();
        validator = vf.getValidator();
        mockMetadataDao = Mockito.mock(MetadataDAO.class);
        ValidationContext.initialize(mockMetadataDao);

       config = Validation.byProvider(HibernateValidator.class).configure();
    }

    @Test
    public void testWorkflowTaskMissingReferenceName() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setTaskReferenceName(null);

        Set<ConstraintViolation<Object>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        assertEquals(result.iterator().next().getMessage(), "WorkflowTask taskReferenceName name cannot be empty or null");
    }

    @Test
    public void testWorkflowTaskTestSetType() throws NoSuchMethodException {
        WorkflowTask workflowTask = createSampleWorkflowTask();

        Method method = WorkflowTask.class.getMethod("setType", String.class);
        Object[] parameterValues = {""};

        ExecutableValidator executableValidator = validator.forExecutables();

        Set<ConstraintViolation<Object>> result = executableValidator.validateParameters(workflowTask, method, parameterValues);

        assertEquals(1, result.size());
        assertEquals(result.iterator().next().getMessage(), "WorkTask type cannot be null or empty");
    }

    @Test
    public void testWorkflowTaskTypeEvent() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("EVENT");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());
        assertEquals(result.iterator().next().getMessage(), "sink field is required for taskType: EVENT taskName: encode");
    }


    @Test
    public void testWorkflowTaskTypeDynamic() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("DYNAMIC");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());
        assertEquals(result.iterator().next().getMessage(), "dynamicTaskNameParam field is required for taskType: DYNAMIC taskName: encode");
    }

    @Test
    public void testWorkflowTaskTypeDecision() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("DECISION");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(2, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("decisionCases should have atleast one task for taskType: DECISION taskName: encode"));
        assertTrue(validationErrors.contains("caseValueParam or caseExpression field is required for taskType: DECISION taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeDoWhile() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("DO_WHILE");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(2, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("loopExpression field is required for taskType: DO_WHILE taskName: encode"));
        assertTrue(validationErrors.contains("loopover field is required for taskType: DO_WHILE taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeDoWhileWithSubWorkflow() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("DO_WHILE");
        workflowTask.setLoopCondition("Test condition");
        WorkflowTask workflowTask2 = createSampleWorkflowTask();
        workflowTask2.setType("SUB_WORKFLOW");
        workflowTask.setLoopOver(Arrays.asList(workflowTask2));

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("SUB_WORKFLOW task inside loopover task is not supported."));
    }

    @Test
    public void testWorkflowTaskTypeDecisionWithCaseParam() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("DECISION");
        workflowTask.setCaseExpression("$.valueCheck == null ? 'true': 'false'");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
            .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
            .buildValidatorFactory()
            .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("decisionCases should have atleast one task for taskType: DECISION taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeForJoinDynamic() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN_DYNAMIC");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(2, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("dynamicForkTasksInputParamName field is required for taskType: FORK_JOIN_DYNAMIC taskName: encode"));
        assertTrue(validationErrors.contains("dynamicForkTasksParam field is required for taskType: FORK_JOIN_DYNAMIC taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeForJoinDynamicLegacy() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN_DYNAMIC");
        workflowTask.setDynamicForkJoinTasksParam("taskList");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    @Test
    public void testWorkflowTaskTypeForJoinDynamicWithForJoinTaskParam() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN_DYNAMIC");
        workflowTask.setDynamicForkJoinTasksParam("taskList");
        workflowTask.setDynamicForkTasksInputParamName("ForkTaskInputParam");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("dynamicForkJoinTasksParam or combination of dynamicForkTasksInputParamName and dynamicForkTasksParam cam be used for taskType: FORK_JOIN_DYNAMIC taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeForJoinDynamicValid() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN_DYNAMIC");
        workflowTask.setDynamicForkTasksParam("ForkTasksParam");
        workflowTask.setDynamicForkTasksInputParamName("ForkTaskInputParam");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    @Test
    public void testWorkflowTaskTypeForJoinDynamicWithForJoinTaskParamAndInputTaskParam() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN_DYNAMIC");
        workflowTask.setDynamicForkJoinTasksParam("taskList");
        workflowTask.setDynamicForkTasksInputParamName("ForkTaskInputParam");
        workflowTask.setDynamicForkTasksParam("ForkTasksParam");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("dynamicForkJoinTasksParam or combination of dynamicForkTasksInputParamName and dynamicForkTasksParam cam be used for taskType: FORK_JOIN_DYNAMIC taskName: encode") );
    }

    @Test
    public void testWorkflowTaskTypeHTTP() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("HTTP");
        workflowTask.getInputParameters().put("http_request", "http://www.netflix.com");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    @Test
    public void testWorkflowTaskTypeHTTPWithHttpParamMissing() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("HTTP");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("inputParameters.http_request field is required for taskType: HTTP taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeHTTPWithHttpParamInTaskDef() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("HTTP");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        TaskDef taskDef = new TaskDef();
        taskDef.setName("encode");
        taskDef.getInputTemplate().put("http_request", "http://www.netflix.com");

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(taskDef);

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }


    @Test
    public void testWorkflowTaskTypeHTTPWithHttpParamInTaskDefAndWorkflowTask() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("HTTP");
        workflowTask.getInputParameters().put("http_request", "http://www.netflix.com");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        TaskDef taskDef = new TaskDef();
        taskDef.setName("encode");
        taskDef.getInputTemplate().put("http_request", "http://www.netflix.com");

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(taskDef);

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    @Test
    public void testWorkflowTaskTypeFork() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("FORK_JOIN");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("forkTasks should have atleast one task for taskType: FORK_JOIN taskName: encode"));
    }


    @Test
    public void testWorkflowTaskTypeSubworkflow() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("SUB_WORKFLOW");

        SubWorkflowParams subWorkflowTask = new SubWorkflowParams();
        workflowTask.setSubWorkflowParam(subWorkflowTask);

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(2, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("SubWorkflowParams name cannot be null"));
        assertTrue(validationErrors.contains("SubWorkflowParams name cannot be empty"));
    }

    @Test
    public void testWorkflowTaskTypeTerminateWithoutTerminationStatus() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType(TaskType.TASK_TYPE_TERMINATE);
        workflowTask.setName("terminate_task");

        workflowTask.setInputParameters(Collections.singletonMap(Terminate.getTerminationWorkflowOutputParameter(), "blah"));
        List<String> validationErrors = getErrorMessages(workflowTask);

        Assert.assertEquals(1, validationErrors.size());
        Assert.assertEquals("terminate task must have an terminationStatus parameter and must be set to COMPLETED or FAILED, taskName: terminate_task", validationErrors.get(0));
    }

    @Test
    public void testWorkflowTaskTypeTerminateWithInvalidStatus() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType(TaskType.TASK_TYPE_TERMINATE);
        workflowTask.setName("terminate_task");

        workflowTask.setInputParameters(Collections.singletonMap(Terminate.getTerminationStatusParameter(), "blah"));

        List<String> validationErrors = getErrorMessages(workflowTask);

        Assert.assertEquals(1, validationErrors.size());
        Assert.assertEquals("terminate task must have an terminationStatus parameter and must be set to COMPLETED or FAILED, taskName: terminate_task", validationErrors.get(0));
    }

    @Test
    public void testWorkflowTaskTypeTerminateOptional() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType(TaskType.TASK_TYPE_TERMINATE);
        workflowTask.setName("terminate_task");

        workflowTask.setInputParameters(Collections.singletonMap(Terminate.getTerminationStatusParameter(), "COMPLETED"));
        workflowTask.setOptional(true);

        List<String> validationErrors = getErrorMessages(workflowTask);

        Assert.assertEquals(1, validationErrors.size());
        Assert.assertEquals("terminate task cannot be optional, taskName: terminate_task", validationErrors.get(0));
    }

    @Test
    public void testWorkflowTaskTypeTerminateValid() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType(TaskType.TASK_TYPE_TERMINATE);
        workflowTask.setName("terminate_task");

        workflowTask.setInputParameters(Collections.singletonMap(Terminate.getTerminationStatusParameter(), "COMPLETED"));

        List<String> validationErrors = getErrorMessages(workflowTask);

        Assert.assertEquals(0, validationErrors.size());
    }

    @Test
    public void testWorkflowTaskTypeKafkaPublish() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("KAFKA_PUBLISH");
        workflowTask.getInputParameters().put("kafka_request", "testInput");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    @Test
    public void testWorkflowTaskTypeKafkaPublishWithRequestParamMissing() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("KAFKA_PUBLISH");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(new TaskDef());

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(1, result.size());

        List<String> validationErrors = new ArrayList<>();

        result.forEach(e -> validationErrors.add(e.getMessage()));

        assertTrue(validationErrors.contains("inputParameters.kafka_request field is required for taskType: KAFKA_PUBLISH taskName: encode"));
    }

    @Test
    public void testWorkflowTaskTypeKafkaPublishWithKafkaParamInTaskDef() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("KAFKA_PUBLISH");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        TaskDef taskDef = new TaskDef();
        taskDef.setName("encode");
        taskDef.getInputTemplate().put("kafka_request", "test_kafka_request");

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(taskDef);

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }


    @Test
    public void testWorkflowTaskTypeKafkaPublioshWithRequestParamInTaskDefAndWorkflowTask() {
        WorkflowTask workflowTask = createSampleWorkflowTask();
        workflowTask.setType("KAFKA_PUBLISH");
        workflowTask.getInputParameters().put("kafka_request", "http://www.netflix.com");

        ConstraintMapping mapping = config.createConstraintMapping();

        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());

        Validator validator = config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();

        TaskDef taskDef = new TaskDef();
        taskDef.setName("encode");
        taskDef.getInputTemplate().put("kafka_request", "test Kafka Request");

        when(mockMetadataDao.getTaskDef(anyString())).thenReturn(taskDef);

        Set<ConstraintViolation<WorkflowTask>> result = validator.validate(workflowTask);
        assertEquals(0, result.size());
    }

    private List<String> getErrorMessages(WorkflowTask workflowTask) {
        Set<ConstraintViolation<WorkflowTask>> result = buildValidator().validate(workflowTask);
        List<String> validationErrors = new ArrayList<>();
        result.forEach(e -> validationErrors.add(e.getMessage()));

        return validationErrors;
    }

    private Validator buildValidator() {
        ConstraintMapping mapping = config.createConstraintMapping();
        mapping.type(WorkflowTask.class)
                .constraint(new WorkflowTaskTypeConstraintDef());
        return config.addMapping(mapping)
                .buildValidatorFactory()
                .getValidator();
    }

    private WorkflowTask createSampleWorkflowTask() {
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("encode");
        workflowTask.setTaskReferenceName("encode");
        workflowTask.setType("FORK_JOIN_DYNAMIC");
        Map<String, Object> inputParam = new HashMap<>();
        inputParam.put("fileLocation", "${workflow.input.fileLocation}");
        workflowTask.setInputParameters(inputParam);
        return workflowTask;
    }
}
