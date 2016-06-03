package org.activiti.rest.listener;

import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.activiti.engine.RuntimeService;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.impl.persistence.entity.IdentityLinkEntity;
import org.activiti.engine.impl.persistence.entity.TaskEntity;
import org.activiti.engine.task.IdentityLink;
import org.activiti.engine.task.IdentityLinkType;
import org.activiti.engine.task.Task;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.env.Environment;
import org.springframework.core.io.support.ResourcePropertySource;

public class TaskEventListener implements ActivitiEventListener {

	private final Logger logger = LoggerFactory.getLogger(TaskEventListener.class);

	private static final String schemaName = "workflow";

	private static final String indexName = "entity_type";

	private static Properties prop;

	public static String solr_url = null;

	public static String zoo_keeper_url = null;

	public static String solr_client_type = null;

	static {
		try {
			prop = new Properties();
			InputStream in = TaskEventListener.class.getClassLoader().getResourceAsStream("engine.properties");
			prop.load(in);
			solr_url = prop.getProperty("engine.search.url");
			zoo_keeper_url = prop.getProperty("search.solr.zoo.keeper.url");
			solr_client_type = prop.getProperty("search.solr.client.type");
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public boolean isFailOnException() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void onEvent(ActivitiEvent event) {

		TaskEntity taskEntity = null;

		logger.debug("event type : {}", event.getType());

		if (event.getType() == ActivitiEventType.TASK_CREATED || event.getType() == ActivitiEventType.TASK_ASSIGNED) {
			Object entity = ((ActivitiEntityEvent) event).getEntity();
			taskEntity = (TaskEntity) entity;
			Map<String, Object> searchVariables = createSearchVariables(taskEntity);
			createIndex(searchVariables);
		}

		if (event.getType() == ActivitiEventType.TASK_COMPLETED) {
			Object entity = ((ActivitiEntityEvent) event).getEntity();
			taskEntity = (TaskEntity) entity;
			removeIndex(taskEntity.getId(), (String) taskEntity.getVariables().get(indexName));
		}

	}

	private Map<String, Object> createSearchVariables(TaskEntity taskEntity) {
		Map<String, Object> searchVariables = taskEntity.getVariables();
		searchVariables.put("id", taskEntity.getId());
		searchVariables.put("entity_id", taskEntity.getId());
		searchVariables.put("entity_type", searchVariables.get(indexName));
		searchVariables.put("index_time", new Date());
		searchVariables.put("TaskDefinitionKey", taskEntity.getTaskDefinitionKey());
		searchVariables.put("TaskName", taskEntity.getName());
		searchVariables.put("TaskAssignee", taskEntity.getAssignee());
		searchVariables.put("ExecutionId", taskEntity.getExecutionId());
		searchVariables.put("ProcessInstanceId", taskEntity.getProcessInstanceId());
		searchVariables.put("TaskCreateTime", taskEntity.getCreateTime());
		searchVariables.put("TaskDueTime", taskEntity.getDueDate());
		searchVariables.put("TaskPrioriry", taskEntity.getPriority());
		searchVariables.put("TaskDescription", taskEntity.getDescription());
		searchVariables.put("AssignBy", taskEntity.getOwner());

		for (IdentityLink identityLink : taskEntity.getCandidates()) {
			if (IdentityLinkType.CANDIDATE.equals(identityLink.getType())) {
				searchVariables.put("Candidates", identityLink.getGroupId());
			}
		}

		return searchVariables;
	}

	private void createIndex(Map<String, Object> searchVariables) {

		SolrInputDocument doc = new SolrInputDocument();
		for (Object obj : searchVariables.keySet()) {
			String fieldName = (String) obj;
			doc.addField(fieldName, searchVariables.get(fieldName));
		}

		SolrClient client = null;
		if ("cloudSolrClient".equals(solr_client_type)) {
			client = new CloudSolrClient(zoo_keeper_url);
			((CloudSolrClient) client).setZkClientTimeout(5000);
			((CloudSolrClient) client).setZkConnectTimeout(5000);
			((CloudSolrClient) client).setDefaultCollection(schemaName);

		} else {
			client = new HttpSolrClient(solr_url + schemaName);
		}
		// SolrClient client = new
		// HttpSolrClient("http://172.25.17.213:8983/solr/" + schemaName);
		try {
			client.add(doc);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("add solr docs failure", e);
			throw new RuntimeException("add solr docs failure");
		}

	}

	private void removeIndex(String taskId, String indexName) {
		String query = "entity_type" + ":" + indexName + " AND " + "id" + ":" + taskId;

		// SolrClient client = new HttpSolrClient(solr_url + schemaName);
		SolrClient client = null;
		if ("cloudSolrClient".equals(solr_client_type)) {
			client = new CloudSolrClient(zoo_keeper_url);
			((CloudSolrClient) client).setZkClientTimeout(5000);
			((CloudSolrClient) client).setZkConnectTimeout(5000);
			((CloudSolrClient) client).setDefaultCollection(schemaName);

		} else {
			client = new HttpSolrClient(solr_url + schemaName);
		}
		try {
			client.deleteByQuery(query);
		} catch (Exception e) {
			logger.error("Error when clear index data.", e);
			throw new RuntimeException("remove solr docs failure");
		}

	}
}
