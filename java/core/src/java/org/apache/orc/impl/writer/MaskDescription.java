package org.apache.orc.impl.writer;

import org.apache.orc.TypeDescription;
import org.apache.orc.DataMask;

public class MaskDescription {
  private final String mask;
  private final String[] parameters;

  public MaskDescription(String mask, String... parameters) {
    this.mask = mask;
    this.parameters = parameters;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    } else if (other != this) {
      MaskDescription mo = (MaskDescription) other;
      if (mask != mo.mask) {
        return false;
      } else if (parameters == mo.parameters) {
        return true;
      } if (parameters == null ||
          mo.parameters == null ||
          parameters.length != mo.parameters.length) {
        return false;
      } else {
        for(int i=0; i < parameters.length; i++) {
          if (!parameters[i].equals(mo.parameters[i])) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = mask.hashCode();
    if (parameters != null) {
      for(String p: parameters) {
        result = result * 31 + p.hashCode();
      }
    }
    return result;
  }

  public String getStyle() {
    return mask;
  }

  public String[] getParameters() {
    return parameters;
  }

  public DataMask create(TypeDescription schema) {
    return DataMask.Factory.build(mask, schema, parameters);
  }
}

