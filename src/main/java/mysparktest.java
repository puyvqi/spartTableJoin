import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public final class mysparktest {
    public static void main(String[] args) throws Exception {
        String tt="sldkfasf;asfasf";
        tt.split(";");
        SparkConf conf =new SparkConf().setMaster("spark://pyq-master:7077").setAppName("hellowspark").setJars(new String[]{"/home/puyvqi/test/tablejoin19/target/sparkjava-1.0-SNAPSHOT.jar"});
       // SparkConf conf =new SparkConf().setMaster("local").setAppName("hellowspark");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> people=sc.textFile("/home/puyvqi/test/tablejoin19/input/people");
        JavaRDD<String> orders=sc.textFile("/home/puyvqi/test/tablejoin19/input/orders");
        System.out.println("people:"+people);
        System.out.println("orders:"+orders);
        people=people.filter(s->(s.length()>0));
        orders=orders.filter(s->(s.length()>0));
        JavaRDD<ArrayList<String>> peoplet=people.map(a->a.split(",")).map(s->new ArrayList<String>(Arrays.asList(s)));
        JavaRDD<ArrayList<String>> orderst=orders.map(a->a.split(",")).map(s->new ArrayList<String>(Arrays.asList(s)));
        JavaPairRDD<String,ArrayList<String>> peoplet2=peoplet.mapToPair(new ex());
        JavaPairRDD<String,ArrayList<String>> orderst2=orderst.mapToPair(new PairFunction<ArrayList<String>, String, ArrayList<String>>(){
            @Override
            public Tuple2<String,ArrayList<String>> call(ArrayList<String> a){
                String foreignkey=a.get(a.size()-1);
                a.remove(a.size()-1);
                return new Tuple2(foreignkey,a);
            }
        });
        JavaPairRDD<String, Tuple2<ArrayList<String>,ArrayList<String>>> kk=orderst2.join(peoplet2);

        kk.repartition(1). saveAsTextFile("/home/puyvqi/test/tablejoin19/result0000");                           ;

    }

}
class ex implements PairFunction<ArrayList<String>,String,ArrayList<String>> {
    @Override
    public Tuple2<String,ArrayList<String>> call(ArrayList<String> a){
        String primarykey=a.get(0);
        a.remove(0);
        return  new Tuple2(primarykey,a);
    }
}
