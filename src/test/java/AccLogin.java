/*
 *
 * @author prasad mokkapati - prasadm80@gmail.com
 */
import java.io.Serializable;
import java.util.Date;
import java.util.ArrayList;

import com.tengo.sqldb.Id;
import com.tengo.sqldb.Table;
import com.tengo.sqldb.OneToMany;


public class AccLogin extends Account {
    private ArrayList<Login> _logins = null;


    @OneToMany(mappedBy={"accountId"})
    public ArrayList<Login> getLogin() {
        return _logins;
    }
    public void setLogin(ArrayList<Login> l) {
        _logins = l;
    }
}
