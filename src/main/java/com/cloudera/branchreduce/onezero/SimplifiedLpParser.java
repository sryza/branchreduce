/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.branchreduce.onezero;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 *
 */
public class SimplifiedLpParser {

  private final List<String> fileContents;
  
  private Expression objective;
  private List<Constraint> constraints;
  private List<String> variablePositions;
  
  public SimplifiedLpParser(String fileData) {
    this.fileContents = Lists.newArrayList(Splitter.on("\n").split(fileData));
  }
  
  public SimplifiedLpParser(File file) throws IOException {
    this.fileContents = Files.readLines(file, Charsets.UTF_8);
  }
  
  public Expression getObjective() {
    return objective;
  }
  
  public List<Constraint> getConstraints() {
    return constraints;
  }
  
  private enum ParseState {
    START,
    OBJECTIVE,
    CONSTRAINTS,
    END
  }
 
  private static class ConstraintData {
    private final Map<String, Integer> coefs;
    private final Integer bound;
    
    public ConstraintData(Map<String, Integer> coefs, Integer bound) {
      this.coefs = coefs;
      this.bound = bound;
    }
    
    public Constraint toConstraint(Map<String, Integer> variableIndices) {
      return new Constraint(create(coefs, variableIndices), bound);
    }
  }
  
  private static class ProblemData {
    private boolean maximize = false;
    private Set<String> variables = Sets.newTreeSet();
    private Map<String, Integer> objectiveCoefs = null;
    private List<ConstraintData> constraints = Lists.newArrayList();
    
    public Set<String> getVariables() {
      return variables;
    }
    
    public Expression getObjective(Map<String, Integer> variableIndices) {
      return create(objectiveCoefs, variableIndices);
    }
    
    public List<Constraint> getConstraints(Map<String, Integer> variableIndices) {
      List<Constraint> c = Lists.newArrayList();
      for (ConstraintData cd : constraints) {
        c.add(cd.toConstraint(variableIndices));
      }
      return c;
    }
    
    public void setMaximize(boolean maximize) {
      this.maximize = maximize;
    }
    
    public void setObjectiveCoefs(Map<String, Integer> coefs) {
      if (maximize) {
        // Convert to a minimization problem
        this.objectiveCoefs = Maps.newHashMap();
        for (Map.Entry<String, Integer> e : coefs.entrySet()) {
          objectiveCoefs.put(e.getKey(), -e.getValue());
        }
      } else {
        this.objectiveCoefs = ImmutableMap.copyOf(coefs);
      }
    }
    
    LineState parseCoefs(Map<String, Integer> currentCoefs, String line, 
        LineState lastLineState) {
      int sign = lastLineState.lastSign;
      boolean limit = false;
      StringTokenizer st = new StringTokenizer(line, "+-<>=", true);
      while (st.hasMoreTokens()) {
        String token = st.nextToken().trim();
        if (token.equals("+")) {
          sign = 1;
        } else if (token.equals("-")) {
          sign = -1;
        } else if (token.equals("<") || token.equals("=") || token.equals(">")) {
          limit = true;
          break;
        } else if (!token.isEmpty()) {
          parseVariable(currentCoefs, token, sign, lastLineState);
          sign = 1;
        }
      }
      return lastLineState.update(sign, limit);
    }
    
    private void parseVariable(Map<String, Integer> currentCoefs, String token,
        int sign, LineState lastLineState) {
      int splitAt = -1;
      for (int i = 0; i < token.length(); i++) {
        char c = token.charAt(i);
        if (!Character.isDigit(c) && c != '.' &&
            !((c == 'e' || c == 'E') &&
                (i < token.length() - 1 && Character.isDigit(token.charAt(i + 1))))) {
          splitAt = i;
          break;
        }
      }
      if (splitAt < 0) {
        throw new IllegalStateException(
            String.format("Parse error for token = %s at line %d", token, lastLineState.lineNumber));
      }
      String variable = token.substring(splitAt);
      Integer coef = splitAt == 0 ? 1 : Integer.valueOf(token.substring(0, splitAt));
      variables.add(variable);
      currentCoefs.put(variable, sign * coef);
    }

    public void addConstraint(Map<String, Integer> currentCoefs, int constraintValue, boolean isUpperBound) {
      if (isUpperBound) {
        // Reverse the sense by negating everything.
        Map<String, Integer> negatedCoefs = Maps.newHashMap();
        for (Map.Entry<String, Integer> e : currentCoefs.entrySet()) {
          negatedCoefs.put(e.getKey(), -e.getValue());
        }
        constraints.add(new ConstraintData(negatedCoefs, -constraintValue));
      } else {
        constraints.add(new ConstraintData(currentCoefs, constraintValue));
      }
    }
  }
  
  public void parse() {
    Iterator<String> lines = fileContents.iterator();
    ParseState state = ParseState.START;
    
    // Global variable info.
    ProblemData problem = new ProblemData();
    
    // Maps variables to coefficients for the current line.
    Map<String, Integer> currentCoefs = Maps.newHashMap();    
    LineState lineState = new LineState();
    
    while (lines.hasNext() && state != ParseState.END) {
      String line = lines.next();
      int commentIndex = line.indexOf("\\");
      if (commentIndex > -1) {
        line = line.substring(0, commentIndex);
      }
      line = line.trim();
      
      int colonIndex = line.indexOf(':');
      if (colonIndex > -1) {
        // Ignore constraint names for now
        line = line.substring(colonIndex + 1);
      }
      
      if (line.length() > 0) {
        if (line.equalsIgnoreCase("end")) {
          state = ParseState.END;
          break;
        } else {
          String cmd = line.toLowerCase();
          switch (state) {
          case START:
            // Anything starting w/'min' means minimize.
            if (cmd.startsWith("min")) {
              problem.setMaximize(false);
            }
            state = ParseState.OBJECTIVE;
            break;
          case OBJECTIVE:
            if (cmd.equals("subject to") || cmd.equals("st") || cmd.equals("s.t.")) {
              problem.setObjectiveCoefs(currentCoefs);
              currentCoefs = Maps.newHashMap();
              state = ParseState.CONSTRAINTS;
            } else {
              lineState = problem.parseCoefs(currentCoefs, line, lineState);
              if (lineState.hasLimit) {
                throw new IllegalStateException("Objective should not a bound (e.g., <=, >=, etc.)");
              }
            }
            break;
          case CONSTRAINTS:
            lineState = problem.parseCoefs(currentCoefs, line, lineState);
            if (lineState.hasLimit) {
              // Means we hit a boundary and need to update.
              String[] pieces = line.split("=");
              if (pieces.length != 2) {
                throw new IllegalStateException("Could not parse line: " + line);
              }
              int constraintValue = Integer.valueOf(pieces[1].trim());
              char bound = pieces[0].charAt(pieces[0].length() - 1);
              if (bound != '<' && bound != '>') {
                throw new IllegalStateException("Constraints must be <= or >= :" + line);
              }
              problem.addConstraint(currentCoefs, constraintValue, bound == '>');
              // Prepare for the next line
              currentCoefs = Maps.newHashMap();
            }
            break;
          }
        }
      }
    }

    // Index all of the variable definitions.
    this.variablePositions = Lists.newArrayList();
    Map<String, Integer> variableIndices = Maps.newHashMap();
    int index = 0;
    for (String varName : problem.getVariables()) {
      variableIndices.put(varName, index++);
      variablePositions.add(varName);
    }

    // Finally done. Now to construct everything.
    this.objective = problem.getObjective(variableIndices);
    this.constraints = problem.getConstraints(variableIndices);
  }

  private static class LineState {
    public int lineNumber;
    public int lastSign;
    public boolean hasLimit;
    
    public LineState() {
      this.lineNumber = 1;
      this.lastSign = 1;
      this.hasLimit = false;
    }
    
    public LineState update(int lastSign, boolean hasLimit) {
      this.lastSign = lastSign;
      this.hasLimit = hasLimit;
      lineNumber++;
      return this;
    }
  }

  private static Expression create(Map<String, Integer> values, Map<String, Integer> variableIndices) {
    int[] coefs = new int[variableIndices.size()];
    for (Map.Entry<String, Integer> e : values.entrySet()) {
      coefs[variableIndices.get(e.getKey())] = e.getValue();
    }
    return Expression.of(coefs);
  }
}
