/*
 *
 * @author prasad mokkapati - prasadm80@gmail.com
 */
import java.io.Serializable;
import java.util.Date;
import com.tengo.sqldb.Id;
import com.tengo.sqldb.Table;


@Table(name="Account")
public class Account implements Serializable {
    public enum AccountType { Unknown, Customer, Agent, Merchant,
            AgentAndMerchant};
    public enum Language { Unknown, English, Spanish };
    public enum Status { Active, Suspended };

    private long _accountId;
    private AccountType  _accountType;
    private String _name;
    private String _telNum;
    private Date   _created;
    private double _balance;
    private Language _language=Language.English;
    private Status _status=Status.Active;

    @Id 
    public long getAccountId() { return _accountId; }
    public void setAccountId(long id) { _accountId = id; }

    public AccountType getAccountType() { return _accountType; }
    public void setAccountType(AccountType id) { _accountType = id; }

    public String getName() { return _name; }
    public void setName(String nm) { _name = nm; }

    public String getTelNum() { return _telNum; }
    public void setTelNum(String num) { _telNum = num; }

    public Date getCreated() { return _created; }
    public void setCreated(Date d) { _created = d; }

    public double getBalance() { return _balance; }
    public void setBalance(double d) { _balance = d; }

    public Status getStatus() { return _status; }
    public void setStatus(Status s) { _status = s; }

    public Language getLanguage() { return _language; }
    public void setLanguage(Language l) { _language = l; }
}
