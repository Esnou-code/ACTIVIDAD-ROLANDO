//package org.myorg;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PalabrasPositivasMapReduce extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new PalabrasPositivasMapReduce(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "palabraspositivas");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    // Un solo reducer para poder ordenar todo el resultado de mayor a menor
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    // Palabras consideradas positivas (se comparan en mayusculas)
    private static final Set<String> POSITIVAS = new HashSet<String>(Arrays.asList(
        "BUENO", "BUENISIMO", "EXCELENTE", "GENIAL", "FANTASTICO",
        "MAGNIFICA", "PERFECTA", "INCREIBLE", "RECOMENDADO"
    ));

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      // Saltamos la fila de encabezado (id,comentario)
      if (offset.get() == 0) {
        return;
      }

      String line = lineText.toString();
      // El campo comentario es todo lo que viene despues de la primera coma
      int primeraComa = line.indexOf(',');
      if (primeraComa < 0) {
        return;
      }
      String comentario = line.substring(primeraComa + 1);

      // Tokenizamos por cualquier caracter que no sea letra
      for (String token : comentario.split("[^A-Za-zÁÉÍÓÚÑáéíóúñ]+")) {
        if (token.isEmpty()) {
          continue;
        }
        String palabra = token.toUpperCase();
        if (POSITIVAS.contains(palabra)) {
          context.write(new Text(palabra), one);
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Acumulamos todos los resultados para poder ordenarlos al final (Top Down)
    private List<Entry<String, Integer>> conteos = new ArrayList<Entry<String, Integer>>();

    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      conteos.add(new java.util.AbstractMap.SimpleEntry<String, Integer>(word.toString(), sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Orden descendente por frecuencia (Top Down)
      Collections.sort(conteos, new Comparator<Entry<String, Integer>>() {
        public int compare(Entry<String, Integer> a, Entry<String, Integer> b) {
          return b.getValue() - a.getValue();
        }
      });
      for (Entry<String, Integer> entry : conteos) {
        context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
      }
    }
  }
}
