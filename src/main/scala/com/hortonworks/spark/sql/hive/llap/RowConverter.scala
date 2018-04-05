/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.spark.sql.hive.llap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.llap.Schema
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.typeinfo._

import org.apache.spark.sql.Row

object RowConverter {
  type ValueConverter = Any => Any

  def makeConverter(schema: Schema): org.apache.hadoop.hive.llap.Row => Row = {
    val fieldConverters = schema.getColumns.asScala.map { colDesc =>
      makeValueConverter(colDesc.getTypeInfo)
    }.toArray

    (llapRow: org.apache.hadoop.hive.llap.Row) => {
      val fields = new ArrayBuffer[Any](fieldConverters.length)
      var idx = 0
      while (idx < fieldConverters.length) {
        fields += fieldConverters(idx)(llapRow.getValue(idx))
        idx += 1
      }
      Row.fromSeq(fields)
    }
  }

  private def makeValueConverter(colType: TypeInfo): ValueConverter = {
    colType.getCategory match {
      // The primitives should not require conversion
      case Category.PRIMITIVE =>
        (value: Any) => if (value == null) {
          null
        } else {
          value
        }

      case Category.LIST =>
        val elementConverter = makeValueConverter(colType.asInstanceOf[ListTypeInfo].getListElementTypeInfo)

        (value: Any) => if (value == null) {
          null
        } else {
          value.asInstanceOf[java.util.List[Any]].asScala.map { listElement =>
            elementConverter(listElement)
          }
        }

      case Category.MAP =>
        val keyConverter = makeValueConverter(colType.asInstanceOf[MapTypeInfo].getMapKeyTypeInfo)
        val valueConverter = makeValueConverter(colType.asInstanceOf[MapTypeInfo].getMapValueTypeInfo)

        (value: Any) => if (value == null) {
          null
        } else {
          // Try LinkedHashMap to preserve order of elements - is that necessary?
          val map = scala.collection.mutable.LinkedHashMap.empty[Any, Any]
          value.asInstanceOf[java.util.Map[Any, Any]].asScala.foreach { case (k, v) =>
            map(keyConverter(k)) = valueConverter(v)
          }
          map
        }

      case Category.STRUCT =>
        val fieldConverters = colType.asInstanceOf[StructTypeInfo]
          .getAllStructFieldTypeInfos.asScala.map(makeValueConverter).toArray

        (value: Any) => if (value == null) {
          null
        } else {
          // Struct value is just a list of values. Convert each value based on corresponding
          // typeinfo
          val fields = new ArrayBuffer[Any](fieldConverters.length)
          val values = value.asInstanceOf[java.util.List[Any]].asScala
          var idx = 0
          while (idx < fieldConverters.length) {
            fields += fieldConverters(idx)(values(idx))
            idx += 1
          }
          Row.fromSeq(fields)
        }

      case _ => (value: Any) => null
    }
  }
}
