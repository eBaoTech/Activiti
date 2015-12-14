package org.activiti.rest.listener;

import java.util.Date;
import java.util.Map;

import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.task.Task;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

public class TaskEventListener implements ActivitiEventListener {

	private final Logger log = LoggerFactory.getLogger(TaskEventListener.class);

	private static final String schemaName = "workflow";
	
	private static final String indexName="entity_type";

	@Autowired
	protected Environment environment;

	@Override
	public boolean isFailOnException() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void onEvent(ActivitiEvent event) {

		Task task = null;

		if (event.getType() == ActivitiEventType.TASK_CREATED || event.getType() == ActivitiEventType.TASK_ASSIGNED) {

			Object entity = ((ActivitiEntityEvent) event).getEntity();
			task = (Task) entity;
			Map<String, Object> searchVariables = createSearchVariables(task);
			createIndex(searchVariables);

		}

		if (event.getType() == ActivitiEventType.TASK_COMPLETED) {

			Object entity = ((ActivitiEntityEvent) event).getEntity();
			task = (Task) entity;
			removeIndex(task.getId(), (String)task.getProcessVariables().get(indexName));

		}

	}

	private Map<String, Object> createSearchVariables(Task task) {
		Map<String, Object> searchVariables = task.getProcessVariables();
		searchVariables.put("id", task.getId());
//		searchVariables.put("entity_type", "Workflow");
		searchVariables.put("entity_type", (String)task.getProcessVariables().get(indexName));
//		searchVariables.put("entity_type", task.getProcessDefinitionId().split(":")[0]);
		searchVariables.put("index_time", new Date());
		searchVariables.put("TaskDefinitionKey", task.getTaskDefinitionKey());
		searchVariables.put("TaskName", task.getName());
		searchVariables.put("TaskAssignee", task.getAssignee());
		searchVariables.put("ExecutionId", task.getExecutionId());
		searchVariables.put("ProcessInstanceId", task.getProcessInstanceId());
		searchVariables.put("TaskCreateTime", task.getCreateTime());
		searchVariables.put("TaskDueTime", task.getDueDate());
		searchVariables.put("TaskPrioriry", task.getPriority());
		return searchVariables;
	}

	private void createIndex(Map<String, Object> searchVariables) {

		SolrInputDocument doc = new SolrInputDocument();
		for (Object obj : searchVariables.keySet()) {
			String fieldName = (String) obj;
			doc.addField(fieldName, searchVariables.get(fieldName));
		}
		SolrClient client = new HttpSolrClient(environment.getProperty("engine.search.url") + schemaName);
		try {
			client.add(doc);
		} catch (Exception e) {
			log.error("add solr docs failure", e);
			throw new RuntimeException("add solr docs failure");
		}

	}

	private void removeIndex(String taskId, String indexName) {

		String query = "entity_type" + ":" + indexName + " AND " + "id" + ":" + taskId;
		try {
			SolrClient client = new HttpSolrClient(environment.getProperty("engine.search.url") + schemaName);
			client.deleteByQuery(query);
		} catch (Exception e) {
			log.error("Error when clear index data.", e);
			throw new RuntimeException("remove solr docs failure");
		}

	}
}
