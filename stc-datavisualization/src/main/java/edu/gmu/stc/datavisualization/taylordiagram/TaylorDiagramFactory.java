package edu.gmu.stc.datavisualization.taylordiagram;


import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import scala.tools.nsc.Global;

/**
 * Created by Fei Hu on 4/17/16.
 */
public class TaylorDiagramFactory {

  public static void drawTaylorDiagram(String pythonscript, String inputPath, String x_axis, String y_axis, String output)
      throws IOException, InterruptedException {
    String command = "Python " + pythonscript + " " + inputPath + " " + x_axis + " " + y_axis + " " + output;
    Process process = Runtime.getRuntime().exec(command);
    process.waitFor();
  }

  public static void savetoJSON(List<TaylorDiagramUnit> units, String output) throws IOException {
    Gson gson = new Gson();
    Writer osWriter = new OutputStreamWriter(new FileOutputStream(output));
    gson.toJson(units, osWriter);
    osWriter.close();
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    List<TaylorDiagramUnit> units = new ArrayList<TaylorDiagramUnit>();
    for (int i=0; i<4; i++) {
      units.add(new TaylorDiagramUnit("test+"+i, new float[]{i,i+1,i+2,i+3}, 0.1f*(5-i), (i+1)/10.0f+0.1f));
    }

    TaylorDiagramFactory.savetoJSON(units, "/Users/feihu/Desktop/taylordiagram.json");
    TaylorDiagramFactory.drawTaylorDiagram("/Users/feihu/Desktop/taylordiagram-v2.py",
                                           "/Users/feihu/Desktop/taylordiagram.json",
                                           "Precipitation",
                                           "values(mm/day)",
                                           "/Users/feihu/Desktop/taylordiagram-test.png");
  }
}
