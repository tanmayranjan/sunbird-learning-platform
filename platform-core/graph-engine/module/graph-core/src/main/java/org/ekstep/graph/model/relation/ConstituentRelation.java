package org.ekstep.graph.model.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ekstep.common.dto.Request;
import org.ekstep.common.exception.ServerException;
import org.ekstep.graph.common.mgr.BaseGraphManager;
import org.ekstep.graph.dac.enums.RelationTypes;
import org.ekstep.graph.dac.enums.SystemNodeTypes;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.exception.GraphRelationErrorCodes;

public class ConstituentRelation extends AbstractRelation {

	public ConstituentRelation(BaseGraphManager manager, String graphId, String startNodeId, String endNodeId) {
		super(manager, graphId, startNodeId, endNodeId);
	}

	@Override
	public void delete(Request request) {
		throw new ServerException(GraphRelationErrorCodes.ERR_RELATION_DELETE.name(),
				"Part of a constituent relation cannot exist alone without reference to the whole");
	}

	@Override
	public String getRelationType() {
		return RelationTypes.CONSTITUENCY.relationName();
	}

	@Override
	public Map<String, List<String>> validateRelation(Request request) {
		try {
			List<String> futures = new ArrayList<String>();
			// check for cycle
			String cyclicCheck = checkCycle(request);
			futures.add(cyclicCheck);

			// Check node types: start node type should be data node.
			// end node type should be data node
			Node startNode = getNode(request, this.startNodeId);
			Node endNode = getNode(request, this.endNodeId);
			String startNodeMsg = getNodeTypeFuture(this.startNodeId, startNode,
					new String[] { SystemNodeTypes.DATA_NODE.name() });
			futures.add(startNodeMsg);
			String endNodeMsg = getNodeTypeFuture(this.endNodeId, endNode,
					new String[] { SystemNodeTypes.DATA_NODE.name() });
			futures.add(endNodeMsg);

			// check if the relation is valid between object type definitions.
			String objectType = getObjectTypeFuture(startNode);
			final String endNodeObjectType = getObjectTypeFuture(endNode);
			String objectTypeMessages = validateObjectTypes(objectType, endNodeObjectType, request);
			futures.add(objectTypeMessages);

			return getMessageMap(futures);
		} catch (Exception e) {
			throw new ServerException(GraphRelationErrorCodes.ERR_RELATION_VALIDATE.name(),
					"Error while validating relation", e);
		}
	}

}
