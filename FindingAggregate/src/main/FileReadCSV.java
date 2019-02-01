package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class FileReadCSV {

	static String file = ".\\src\\resources\\Data.csv";

	static BufferedReader br;
	static BufferedWriter bw;
	static BufferedWriter baggregate;

	static void openfileforreading() {

		try {
			br = new BufferedReader(new FileReader(file));

			// line is not visible here.
		} catch (Exception e) {
			System.out.println("Could not open the file for processing");
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		final long READ_WRITE_INTERVAL = 3000L;
		final long TESTING_SLOWNESS_VISIBILITY_INTERVAL = 100L;
		final boolean DEBUG = false;

		TimerTask repeatedTask = new TimerTask() {
			public void run() {

				long intervalstart = System.currentTimeMillis();
				long timenow = System.currentTimeMillis();
				try {
					if (br == null) {
						openfileforreading();
					} else {

						String str;
						Double total = 0.0;
						int count = 0;
						do {
							str = br.readLine();
							if (str != null) {
								total += Double.parseDouble(str);
								count += 1;
								if (DEBUG) {
									System.out.println("Read:" + str + ":" + count);
								}
							} else {
								break;
							}
							// System.out.println("Sleeping..");
							Thread.sleep(TESTING_SLOWNESS_VISIBILITY_INTERVAL);
							timenow = System.currentTimeMillis();

						} while (timenow < intervalstart + READ_WRITE_INTERVAL);

						if (count != 0) {
							if (DEBUG) {
								System.out.println("Read:" + "total:" + total + ":" + count);
							}
							AggregateCSV a = new AggregateCSV(total, count);
							a.start();
						}

					}

				} catch (Exception e) {
					System.out.println(e);
				}
			}
		};

		// Write contents to file
		TimerTask WriteTask = new TimerTask() {
			public void run() {

				long intervalstart = System.currentTimeMillis();
				try {
					bw = new BufferedWriter(new FileWriter(file, true));

					do {
						// System.out.println("Writing..");
						int range = 100 - 10 + 1;
						// Generate random number
						Double D = Math.random() * range + 1;
						String value = D.toString();
						// System.out.println(value);
						bw.append(value);
						bw.newLine();
						Thread.sleep(TESTING_SLOWNESS_VISIBILITY_INTERVAL);
					} while (System.currentTimeMillis() < intervalstart + READ_WRITE_INTERVAL);
					bw.close();

				} catch (Exception e) {
					System.out.println(e);
				} finally {
				}
			}
		};

		try {
			Timer writetimer = new Timer("Timer");
			Timer readtimer = new Timer("Timer");
			// task, initial delay, scheduling interval
			writetimer.scheduleAtFixedRate(WriteTask, 0L, READ_WRITE_INTERVAL);
			// task, initial delay, scheduling interval
			readtimer.scheduleAtFixedRate(repeatedTask, 1000L, READ_WRITE_INTERVAL);

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}

class AggregateCSV extends Thread {

	Double total;
	int count;

	String aggregatefilePath = ".\\src\\resources\\Aggregate.csv";

	AggregateCSV(Double total, int count) {
		this.count = count;
		this.total = total;
	}

	AggregateCSV() {
		this.total = 0.0;
		this.count = 0;
	}

	public void run() {
		System.out.println("Entering aggregate thread");
		// BufferedWriter baggregate;
		try {
			BufferedWriter baggregate = new BufferedWriter(new FileWriter(aggregatefilePath, true));
			BufferedReader baggreader = new BufferedReader(new FileReader(aggregatefilePath));

			if (baggregate != null && baggreader != null) {
				// Write header if the file is empty
				if (baggreader.readLine() == null) {
					baggregate.append("Average" + "," + "# of rows");
					baggregate.newLine();
				}
				if (count != 0) {
					Double avg = total / count;
					baggregate.append(avg.toString() + "," + count + " rows");
					baggregate.newLine();
					System.out.println("Aggreagate:" + "Count:" + count + ":Average=" + avg.toString());
				}
				baggreader.close();
				baggregate.close();
			} else {
				System.out.println("Error in opening reader or writer for aggregate");
			}
		} catch (Exception e) {
			System.out.println("Could not open the AGGREGATE file for processing");
			e.printStackTrace();
		}

	}

}
