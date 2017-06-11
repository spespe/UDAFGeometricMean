package UDAFGeometricMean;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Create by Pietro Speri 04/06/2017
 * Enjoy!
 *
 */
public class UDAFGeometricMean extends AbstractGenericUDAFResolver
{
    static final Log log = LogFactory.getLog(UDAFGeometricMean.class.getName());


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo [] parameters) throws SemanticException{
        //Parameters check
        if(parameters.length!=3){
            throw new UDFArgumentException("Parameters: " + (parameters.length-1) + ". The number of parameters needs to be equal to 3.");
        }

        if(parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("Only numeric types are allowed! Instead the given input was: "+parameters[0].getTypeName());
        }

        return new UDAFGeometricMeanEvaluator();
    }

    public static class UDAFGeometricMeanEvaluator extends GenericUDAFEvaluator{
        //To be implemented
        //@Override
        public reset(VectorAggregateExpression.AggregationBuffer){

        }
   }
}
