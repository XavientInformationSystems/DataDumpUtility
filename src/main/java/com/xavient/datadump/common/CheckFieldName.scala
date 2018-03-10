package com.xavient.datadump.common

import scala.util.matching.Regex


class CheckFieldName {
  
  
  
def  getValidNewFieldName(originalName :String ) : String = {
  
    val pattern = new Regex("[^a-z _A-Z0-9]")

    val newName = pattern.replaceAllIn(originalName, "")

    newName.toLowerCase().replace(" ", "_").trim()
  
}
  
  
}