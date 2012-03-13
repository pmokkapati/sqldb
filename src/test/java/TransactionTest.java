import com.google.inject.Inject;

import com.tengo.sqldb.*;
import com.tengo.sqldb.inject.module.TransactionModule;

public class TransactionTest {
    private DBManager _mgr = null;

    @Inject
    public TransactionTest(DBManager mgr) {
        _mgr = mgr;
    }

    @Transactional
    public void updateAccount() throws Exception {
        Account c = _mgr.get(Account.class, 
            "select * from Account where accountId = 1 ");
        c.setBalance(c.getBalance() + 10);
        _mgr.update(c);
    }
    @Transactional
    public void updateAccountFail() throws Exception {
        Account c = _mgr.get(Account.class, 
            "select * from Account where accountId = 1 ");
        c.setBalance(-99);
        _mgr.update(c);
        c = _mgr.get(Account.class, 
            "select * from Account where accountId = 1 ");
        throw new Exception("Test");
    }
}
