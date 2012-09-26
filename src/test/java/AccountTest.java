/*
 *
 * @author prasad mokkapati - prasadm80@gmail.com
 */
import java.util.ArrayList;
import java.util.Date;
import java.sql.Timestamp;
import org.junit.*;
import com.google.inject.Guice;
import com.google.inject.Injector;

import com.tengo.sqldb.*;
import com.tengo.sqldb.inject.module.TransactionModule;

public class AccountTest {
    private Injector _inj = null;
    private DBManager _mgr = null;

    @Before
    public void init() {
        _inj = Guice.createInjector(new DBModule(), 
                new TransactionModule());
        _mgr = _inj.getInstance(DBManager.class);
    }

        

    /*
    @Test
    public void doSimpleTest() throws Exception {
        Account n = new Account();
        n.setName("Merchant One");
        n.setAccountType(Account.AccountType.Merchant);
        n.setTelNum("14135314937");
        n.setStatus(Account.Status.Active);
        n.setCreated(new Date());
        n.setLanguage(Account.Language.Spanish);
        _mgr.begin();
        _mgr.insert(n);


        ArrayList<Account> list = 
            _mgr.select(Account.class, "select * from Account");
        System.out.println("Num of items in list: " + list.size());
        for (Account a: list) {
            System.out.println("Id: " + a.getAccountId() 
                + ", name: '" + a.getName() + "', "
                + ", accountType: '" + a.getAccountType() + "' "
                + ", Language: '" + a.getLanguage() + "' "
                + ", Status: '" + a.getStatus() + "' "
                + ", Balance: '" + a.getBalance() + "' "
                + ", telNum: '" + a.getTelNum() + "'");
        }
        n.setBalance(100);
        n.setLanguage(Account.Language.English);
        _mgr.update(n);
        list = _mgr.select(Account.class, "select * from Account");
        for (Account a: list) {
            System.out.println("Id: " + a.getAccountId() 
                + ", name: '" + a.getName() + "', "
                + ", accountType: '" + a.getAccountType() + "' "
                + ", Language: '" + a.getLanguage() + "' "
                + ", Status: '" + a.getStatus() + "' "
                + ", Balance: '" + a.getBalance() + "' "
                + ", telNum: '" + a.getTelNum() + "'");
        }
        _mgr.rollback();
    }
    @Test
    public void doUpdateTrans() throws Exception {
        TransactionTest t = _inj.getInstance(TransactionTest.class);
        t.updateAccount();
    }
    @Test
    public void doUpdateTransFail() throws Exception {
        TransactionTest t = _inj.getInstance(TransactionTest.class);
        try {
            t.updateAccountFail();
        }
        catch (Exception e) {
        }
    }
    */
    @Test
    public void doOneToManyTest() throws Exception {
        AccLogin al = _mgr.get(AccLogin.class, 
            "select a.*, l.loginId, l.password, l.userType, l.userid "
            + " from account a join login l using(accountID) "
            + " where a.accountID=1");
        Assert.assertNotNull(al);
        ArrayList<Login> logins = al.getLogin();
        System.out.println("Size: " + logins.size());
        for (Login l: logins) {
            System.out.println("UserType: " + l.getUserType()
                + ", userid: " + l.getUserid());
        }
    }
}
