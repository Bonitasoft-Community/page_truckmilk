package org.bonitasoft.truckmilk.job;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.service.TenantServiceAccessor;

/* ******************************************************************************** */
/*                                                                                  */
/* Context */
/*                                                                                  */
/*                                                                                  */
/* Contains all contextual information (tenantId, apiAccessor)                      */
/* ******************************************************************************** */

public class MilkJobContext {
    
    private long tenantId;
    private APIAccessor apiAccessor = null;
    private TenantServiceAccessor tenantServiceAccessor=null;

    public MilkJobContext(long tenantId, APIAccessor apiAccessor, TenantServiceAccessor tenantServiceAccessor) {
        this.tenantId = tenantId;
        this.apiAccessor = apiAccessor;
        this.tenantServiceAccessor = tenantServiceAccessor;
    }
    public MilkJobContext(long tenantId ) {
        this.tenantId = tenantId;
    }
    
    public long getTenantId() {
        return this.tenantId;
    }
    public APIAccessor getApiAccessor() {
        return apiAccessor;
    }
    
    public TenantServiceAccessor getTenantServiceAccessor() {
        return tenantServiceAccessor;
    }

}
