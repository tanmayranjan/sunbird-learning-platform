package org.ekstep.taxonomy.mgr.impl;

import static org.junit.Assert.assertEquals;


import org.ekstep.common.dto.Response;
import org.ekstep.common.exception.ResponseCode;
import org.ekstep.content.mgr.impl.operation.dialcodes.ReleaseDialcodesOperation;
import org.ekstep.learning.hierarchy.store.HierarchyStore;
import org.ekstep.taxonomy.util.DialCodeTestInputUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;


public class ReleaseDialcodesOperationTest {

    private HierarchyStore hierarchyStoreMock;

    public ReleaseDialcodesOperation operation;


    @Before
    public void setUp() throws Exception {
        hierarchyStoreMock = Mockito.mock(HierarchyStore.class);
        operation = new ReleaseDialcodesOperation(hierarchyStoreMock);
    }

    @After
    public void tearDown() throws Exception {
        hierarchyStoreMock = null;
        operation = null;
    }

    @Ignore
    @Test
    public void testReleaseDialCode2() throws Exception {
        Mockito.when(hierarchyStoreMock.getHierarchy(Mockito.anyString())).thenReturn(DialCodeTestInputUtil.getMap(DialCodeTestInputUtil.HIERARCHY_WITH_RESERVED_DC_2));
        Response response = operation.releaseDialCodes(DialCodeTestInputUtil.CONTENT_ID, DialCodeTestInputUtil.CHANNEL_ID);
        assertEquals(response.getResponseCode(), ResponseCode.OK);
    }

    @Ignore
    @Test
    public void testReleaseDialCode1() throws Exception {
        Mockito.when(hierarchyStoreMock.getHierarchy(Mockito.anyString())).thenReturn(DialCodeTestInputUtil.getMap(DialCodeTestInputUtil.HIERARCHY_WITH_RESERVED_DC_1));
        Response response = operation.releaseDialCodes(DialCodeTestInputUtil.CONTENT_ID, DialCodeTestInputUtil.CHANNEL_ID);
        assertEquals(response.getResponseCode(), ResponseCode.OK);

    }

}
