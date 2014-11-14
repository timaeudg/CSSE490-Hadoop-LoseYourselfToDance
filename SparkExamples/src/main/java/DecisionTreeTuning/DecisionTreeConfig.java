package DecisionTreeTuning;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;

public class DecisionTreeConfig {
    private DecisionTreeModel model;
    private int bins;
    private int depth;
    private String function;
    private int classes;
    private Double error;
  
    public DecisionTreeConfig(DecisionTreeModel model, int bins, int depth, String function, int classes, double trainError) {
        super();
        this.model = model;
        this.bins = bins;
        this.depth = depth;
        this.function = function;
        this.classes = classes;
        this.error = trainError;
    }
    
    public DecisionTreeModel getModel() {
        return model;
    }
    
    public void setModel(DecisionTreeModel model) {
        this.model = model;
    }
    
    public int getBins() {
        return bins;
    }
    
    public void setBins(int bins) {
        this.bins = bins;
    }
    
    public int getDepth() {
        return depth;
    }
    
    public void setDepth(int depth) {
        this.depth = depth;
    }
    
    public String getFunction() {
        return function;
    }
    
    public void setFunction(String function) {
        this.function = function;
    }
    
    public Double getError() {
        return error;
    }

    public void setError(Double error) {
        this.error = error;
    }

    public int getClasses() {
        return classes;
    }
    
    public void setClasses(int classes) {
        this.classes = classes;
    }
    
    public boolean hasLessError(DecisionTreeConfig other) {
        if (other == null) return true;
        return this.getError() < other.getError();
    }

    @Override
    public String toString() {
        String outputString = "";
        
        outputString += "Number of Classes: " + this.getClasses() + "\n";
        outputString += "Number of Bins: " + this.getBins() + "\n";
        outputString += "Depth: " + this.getDepth() + "\n";
        outputString += "Error: " + this.getError() + "\n";
        outputString += "Error Function: " + this.getFunction() + "\n";
        outputString += "Model:\n" + this.getModel().toString() + "\n";
        
        return outputString;
    }
}
