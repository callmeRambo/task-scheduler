import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import net.datastructures.AdaptablePriorityQueue;
import net.datastructures.Entry;
import net.datastructures.HeapAdaptablePriorityQueue;
import net.datastructures.HeapPriorityQueue;

/**
 * @author zyc,z5098663
 *
 */
public class TaskScheduler {
	static void scheduler(String file1, String file2, int m) 
    {
		HeapPriorityQueue<Integer,Task> hpq = new HeapPriorityQueue<Integer,Task>();
		hpq = read(file1);
		String result = proceeding(hpq,m);
		write(result,file2);
	}
	//in this method, we read data from txt file and store in HeapPriorityQueue
	//the time complexity should be nlogn due to because we need to insert n elements, each took logn.
	public static HeapPriorityQueue<Integer,Task> read (String file1){
		String s;
		HeapPriorityQueue<Integer,Task> hpq = new HeapPriorityQueue<Integer,Task>();
		try {
			FileReader fileReader = new FileReader(file1);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ((s = bufferedReader.readLine()) != null) {
				//split("\\s+") split all kind of space there
				String[] data = s.split("\\s+");
				for (int i = 0; i <= data.length-4; i=i+4) {
					Task t1 = new Task(data[i],Integer.parseInt(data[i+1]),
							Integer.parseInt(data[i+2]),Integer.parseInt(data[i+3]));
					hpq.insert(t1.r,t1);
				}
			}
		} catch (Exception e) {
			System.out.println(file1+" does not exist");
			System.exit(0);
			// TODO: handle exception
		}
		return hpq;
	}
	//proceeding method we use 3 queues, cores_queue, hpq2 as deadline queue.
	//So for every time unit, we'll visit the release time queue, if the top of 
	//release time queue fits the time unit, it will be dumped to another queue,
	//deadline queue, and with deadline as key.
	//Therefore, for different time unit, we can get the task with earliest deadline.
	//The time complexity is nlogn.
	//Generating HeapAdaptablePriorityQueue and HeapPriorityQueue is nlogn.
	//HeapAdaptablePriorityQueue.replaceKey is logn, to replace n elements, took nlogn
	public static String proceeding(HeapPriorityQueue<Integer,Task> hpq, int m){
		HeapAdaptablePriorityQueue<Integer,String> cores = new HeapAdaptablePriorityQueue<Integer,String>();
		HeapPriorityQueue<Integer,Task> hpq2 = new HeapPriorityQueue<Integer,Task>();
		String result = "";
		for (int i = 0; i < m; i++) {
			cores.insert(0, i+1+"");
		}
		for (int i = 0;; i++){
			//time
			if (hpq.isEmpty() && hpq2.isEmpty()){
				break;
			}
			while(hpq.size()>0){
				if (i==hpq.min().getValue().getR()){
					hpq2.insert(hpq.min().getValue().getD(), hpq.min().getValue());
					//removeMin step take O(logn) each time, nlogn in total. 
					hpq.removeMin();
				}
				else
					break;
			}
			while(cores.min().getKey()<=i && !hpq2.isEmpty()){
				if(hpq2.min().getKey()-i<hpq2.min().getValue().getC()){
					return "There is no feasible schedule on "+m+" cores";
				}
				result+=hpq2.min().getValue().getV()+" Core"+cores.min().getValue()+" "+i+"  ";
				cores.replaceKey(cores.min(), hpq2.min().getValue().getC()+i);
				hpq2.removeMin();
			}
		}
		return result;
	}

	public static void write (String result,String file2){
	     try{
		      FileWriter fileWritter = new FileWriter(file2+".txt",false);
		      BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
		      bufferWritter.write(result);
		      bufferWritter.close();
		     }catch(IOException e){
		      e.printStackTrace();
		     }
	}
	
	/**
	 * @author zyc
	 * Class task has 4 attributes
	 * v,c,r,d
	 */ 
	static class Task {
		String v;
		int c;
		int r;
		int d;
		public Task(String v, int c, int r, int d) {
			this.v = v;
			this.c = c;
			this.r = r;
			this.d = d;
		}
		public String getV() {
			return v;
		}
		public int getC() {
			return c;
		}
		public int getR() {
			return r;
		}
		public int getD() {
			return d;
		}
		public void setC(){
			this.c--;
		}

	} /** Put all the additional classes you need here */
	

	/**
	 *
	 * @author Hui Wu
	 */
	  
	  public static void main(String[] args) throws Exception{

	    TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule1", 4);
	   /** There is a feasible schedule on 4 cores */      
	    TaskScheduler.scheduler("samplefile1.txt", "feasibleschedule2", 3);
	   /** There is no feasible schedule on 3 cores */
	    TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule3", 5);
	   /** There is a feasible scheduler on 5 cores */ 
	    TaskScheduler.scheduler("samplefile2.txt", "feasibleschedule4", 4);
	   /** There is no feasible schedule on 4 cores,mission impossible */

	   /** There is no feasible scheduler on 2 cores */ 
	    TaskScheduler.scheduler("samplefile3.txt", "feasibleschedule5", 2);
	    /** There is a feasible scheduler on 2 cores */ 
	    TaskScheduler.scheduler("samplefile4.txt", "feasibleschedule6", 2);
	    
	    
	    TaskScheduler.scheduler("samplefile5.txt", "feasibleschedule7", 2);

	  }


}
