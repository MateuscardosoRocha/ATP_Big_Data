/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atpone.ap;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author r.cardoso4
 */
public class Informacao7 {
    
    public static class MapperInformacao7 extends Mapper<Object, Text, Text, IntWritable>{
    
    @Override
    public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
        String linha = valor.toString();
        String[] campos = linha.split(";");        
        
        if(campos.length == 10){
            String mercadoria = campos[3].trim();
            String peso = campos[6];
            
            IntWritable valorMap = new IntWritable(0);
            Text chaveMap = new Text(mercadoria);
            
            try{
                valorMap = new IntWritable(Integer.parseInt(peso));  
            }catch(NumberFormatException e){
                e.printStackTrace();
            }finally{
                context.write(chaveMap, valorMap);
            }           
        }
    }
 }
    
    
    public static class ReducerInformacao7 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Text mercadoria = new Text();
        private final IntWritable maiorPeso = new IntWritable(0);
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context){
            int peso = 0;
            
            for(IntWritable valor: valores){
                peso += valor.get();
            }
            
            if(peso > maiorPeso.get()){
                maiorPeso.set(peso);
                mercadoria.set(chave);
            }
            
        }
        
        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            context.write(new Text("Mercadoria com maior total de peso: "), maiorPeso);
            context.write(mercadoria, maiorPeso);
        }
    }
    
   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/gilson.edivaldo/Desktop/atp/informacao7";

        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao7");

        job.setJarByClass(Informacao7.class);
        job.setMapperClass(MapperInformacao7.class);
        job.setReducerClass(ReducerInformacao7.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}
