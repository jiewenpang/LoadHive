package com.asiainfo.util;

import java.util.HashSet;
import java.util.HashMap;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class CheckDup {
	private HashMap<String, HashSet<String>> checkMap;
	private String checkPath;
	private String progName;
	private String checkOrNot;

	public CheckDup(String checkPath, String programNo, String flag) {
		this.checkMap = new HashMap<String, HashSet<String>>();
		this.checkPath = checkPath;
		this.progName = programNo;
		this.checkOrNot = flag;
	}
	
	public boolean checkfile(String filename, String day) {
		if (!checkOrNot.equals("1")) {
			return true;
		}
		
		HashSet<String> fileSet = checkMap.get(day);
		if (fileSet == null) {
			loadCheckSet(day);
			fileSet = checkMap.get(day);
		}
		
		if (fileSet.contains(filename)) {
			return false;
		}
		return true;
	}

	public void loadCheckSet(String day) {
		String fileName = checkPath + "/" + "check_" + progName + "_" + day;
		File file = new File(fileName);
		BufferedReader reader = null;
		HashSet<String> fileSet = new HashSet<String>();

		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				String[] sourceStrArray = tempString.split("\\|");
				if (sourceStrArray.length != 2) {
					continue;
				}
				fileSet.add(sourceStrArray[1]);
			}
			reader.close();
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			checkMap.put(day, fileSet);
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	public int Addfile(String filename, String day) {
		WriteFile(day, filename);
		if (!checkOrNot.equals("1")) {
			return 0;
		}
		
		HashSet<String> fileSet = checkMap.get(day);
		if (fileSet == null) {
			fileSet = new HashSet<String>();
			checkMap.put(day, fileSet);
		} 
		fileSet.add(filename);

		return 0;
	}

	public int WriteFile(String DealDay, String filename) {
		PrintWriter writer;
		try {
			writer = new PrintWriter(new FileWriter(checkPath + "/" + "check_" + progName + "_" + DealDay, true));
			writer.println(DealDay + "|" + filename);
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}
