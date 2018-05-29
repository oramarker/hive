
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveJDBC {

    private static void assertFileExist(String file){
        if(!new File(file).exists()){
            throw new RuntimeException("File not found:"+file);
        }
    }

    public static  Connection getHiveConnection(String krb5Conf,String principal,String keyTabFile, String jdbcURL) throws Exception{

        assertFileExist(krb5Conf);
        assertFileExist(keyTabFile);
        System.setProperty("java.security.krb5.conf",krb5Conf);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keyTabFile);
        Connection con = DriverManager.getConnection(jdbcURL);
        return con;

    }

   
