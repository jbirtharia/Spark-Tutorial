package rdd.common;

/**
 * @author JayendraB
 * Created on 21/11/21
 */
public class Utils {

    private Utils(){
        // Default Constructor
    }

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

}
