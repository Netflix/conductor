package com.netflix.conductor.core.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;
import com.netflix.conductor.annotations.Service;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.config.ValidationModule;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.TerminateWorkflowException;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.interceptors.ServiceInterceptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.netflix.conductor.utility.TestUtils.getConstraintViolationMessages;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetadataMapperServiceTest {

    @Mock
    private MetadataDAO metadataDAO;

    private MetadataMapperService metadataMapperService;

    @Before
    public void before() {
        metadataMapperService = Mockito.mock(MetadataMapperService.class);
        Injector injector =
                Guice.createInjector(
                        new AbstractModule() {
                            @Override
                            protected void configure() {
                                bind(MetadataDAO.class).toInstance(metadataDAO);
                                install(new ValidationModule());
                                bindInterceptor(Matchers.any(), Matchers.annotatedWith(Service.class), new ServiceInterceptor(getProvider(Validator.class)));
                            }
                        });
        metadataMapperService = injector.getInstance(MetadataMapperService.class);
    }

    @Test
    public void testMetadataPopulationOnSimpleTask() {
        String nameTaskDefinition = "task1";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition);
        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);

        when(metadataDAO.getTaskDef(nameTaskDefinition)).thenReturn(taskDefinition);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        WorkflowTask populatedWorkflowTask = workflowDefinition.getTasks().get(0);
        assertNotNull(populatedWorkflowTask.getTaskDefinition());
        verify(metadataDAO).getTaskDef(nameTaskDefinition);
    }

    @Test
    public void testNoMetadataPopulationOnEmbeddedTaskDefinition() {
        String nameTaskDefinition = "task2";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition);
        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setTaskDefinition(taskDefinition);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        WorkflowTask populatedWorkflowTask = workflowDefinition.getTasks().get(0);
        assertNotNull(populatedWorkflowTask.getTaskDefinition());
        verifyNoInteractions(metadataDAO);
    }

    @Test
    public void testMetadataPopulationOnlyOnNecessaryWorkflowTasks() {
        String nameTaskDefinition1 = "task4";
        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition1);
        WorkflowTask workflowTask1 = createWorkflowTask(nameTaskDefinition1);
        workflowTask1.setTaskDefinition(taskDefinition);

        String nameTaskDefinition2 = "task5";
        WorkflowTask workflowTask2 = createWorkflowTask(nameTaskDefinition2);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask1, workflowTask2));

        when(metadataDAO.getTaskDef(nameTaskDefinition2)).thenReturn(taskDefinition);

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(2, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        assertNotNull(workflowTasks.get(0).getTaskDefinition());
        assertNotNull(workflowTasks.get(1).getTaskDefinition());

        verify(metadataDAO).getTaskDef(nameTaskDefinition2);
        verifyNoMoreInteractions(metadataDAO);
    }

    @Test(expected = ApplicationException.class)
    public void testMetadataPopulationMissingDefinitions() {
        String nameTaskDefinition1 = "task4";
        WorkflowTask workflowTask1 = createWorkflowTask(nameTaskDefinition1);

        String nameTaskDefinition2 = "task5";
        WorkflowTask workflowTask2 = createWorkflowTask(nameTaskDefinition2);

        TaskDef taskDefinition = createTaskDefinition(nameTaskDefinition1);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask1, workflowTask2));

        when(metadataDAO.getTaskDef(nameTaskDefinition1)).thenReturn(taskDefinition);
        when(metadataDAO.getTaskDef(nameTaskDefinition2)).thenReturn(null);

        metadataMapperService.populateTaskDefinitions(workflowDefinition);
    }

    @Test
    public void testVersionPopulationForSubworkflowTaskIfVersionIsNotAvailable() {
        String nameTaskDefinition = "taskSubworkflow6";
        String workflowDefinitionName = "subworkflow";
        Integer version = 3;

        WorkflowDef subWorkflowDefinition = createWorkflowDefinition("workflowDefinitionName");
        subWorkflowDefinition.setVersion(version);

        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName(workflowDefinitionName);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask));

        when(metadataDAO.getLatest(workflowDefinitionName)).thenReturn(Optional.of(subWorkflowDefinition));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        SubWorkflowParams params = workflowTasks.get(0).getSubWorkflowParam();

        assertEquals(workflowDefinitionName, params.getName());
        assertEquals(version, params.getVersion());

        verify(metadataDAO).getLatest(workflowDefinitionName);
        verify(metadataDAO).getTaskDef(nameTaskDefinition);
        verifyNoMoreInteractions(metadataDAO);
    }

    @Test
    public void testNoVersionPopulationForSubworkflowTaskIfAvailable() {
        String nameTaskDefinition = "taskSubworkflow7";
        String workflowDefinitionName = "subworkflow";
        Integer version = 2;

        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName(workflowDefinitionName);
        subWorkflowParams.setVersion(version);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask));

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        assertEquals(1, workflowDefinition.getTasks().size());
        List<WorkflowTask> workflowTasks = workflowDefinition.getTasks();
        SubWorkflowParams params = workflowTasks.get(0).getSubWorkflowParam();

        assertEquals(workflowDefinitionName, params.getName());
        assertEquals(version, params.getVersion());

        verify(metadataDAO).getTaskDef(nameTaskDefinition);
        verifyNoMoreInteractions(metadataDAO);
    }


    @Test(expected = TerminateWorkflowException.class)
    public void testExceptionWhenWorkflowDefinitionNotAvailable() {
        String nameTaskDefinition = "taskSubworkflow8";
        String workflowDefinitionName = "subworkflow";

        WorkflowTask workflowTask = createWorkflowTask(nameTaskDefinition);
        workflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        SubWorkflowParams subWorkflowParams = new SubWorkflowParams();
        subWorkflowParams.setName(workflowDefinitionName);
        workflowTask.setSubWorkflowParam(subWorkflowParams);

        WorkflowDef workflowDefinition = createWorkflowDefinition("testMetadataPopulation");
        workflowDefinition.setTasks(ImmutableList.of(workflowTask));

        when(metadataDAO.getLatest(workflowDefinitionName)).thenReturn(Optional.empty());

        metadataMapperService.populateTaskDefinitions(workflowDefinition);

        verify(metadataDAO).getLatest(workflowDefinitionName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupWorkflowDefinition() {
        try {
            String workflowName = "test";
            when(metadataDAO.get(workflowName, 0)).thenReturn(Optional.of(new WorkflowDef()));
            Optional<WorkflowDef> optionalWorkflowDef = metadataMapperService.lookupWorkflowDefinition(workflowName, 0);
            assertTrue(optionalWorkflowDef.isPresent());
            metadataMapperService.lookupWorkflowDefinition(null, 0);
        } catch (ConstraintViolationException ex) {
            Assert.assertEquals(1, ex.getConstraintViolations().size());
            Set<String> messages = getConstraintViolationMessages(ex.getConstraintViolations());
            assertTrue(messages.contains("WorkflowIds list cannot be null."));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupLatestWorkflowDefinition() {
        String workflowName = "test";
        when(metadataDAO.getLatest(workflowName)).thenReturn(Optional.of(new WorkflowDef()));
        Optional<WorkflowDef> optionalWorkflowDef = metadataMapperService.lookupLatestWorkflowDefinition(workflowName);
        assertTrue(optionalWorkflowDef.isPresent());

        metadataMapperService.lookupLatestWorkflowDefinition(null);
    }

    @Test
    public void testShouldNotPopulateTaskDefinition() {
        WorkflowTask workflowTask = createWorkflowTask("");
        assertFalse(metadataMapperService.shouldPopulateTaskDefinition(workflowTask));
    }

    @Test
    public void testShouldPopulateTaskDefinition() {
        WorkflowTask workflowTask = createWorkflowTask("test");
        assertTrue(metadataMapperService.shouldPopulateTaskDefinition(workflowTask));
    }

    private WorkflowDef createWorkflowDefinition(String name) {
        WorkflowDef workflowDefinition = new WorkflowDef();
        workflowDefinition.setName(name);
        return workflowDefinition;
    }

    private WorkflowTask createWorkflowTask(String name) {
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName(name);
        workflowTask.setType(TaskType.SIMPLE.name());
        return workflowTask;
    }

    private TaskDef createTaskDefinition(String name) {
        TaskDef taskDefinition = new TaskDef(name);
        return taskDefinition;
    }
}
