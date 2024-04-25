/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  if(result_tuple_.cell_num()>0){
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  std::vector<Value>result_cells((int)aggregations_.size());
  //Value cell_now;
  //result_cells.push_back(cell_now);

  float answer=0;
  float sum=0;
  float answer_min=-999;
  float answer_max=999;
  float index=0;
  string max_str="~~~~~";
  string min_str="";
  char ans_char[20];

  while (RC::SUCCESS == (rc=oper->next())){
    Tuple *tuple =oper->current_tuple();
    if(nullptr == tuple){
      LOG_WARN("failed to get current record : %s",strrc(rc));
      return rc;
    }

    for(int cell_idx=0;cell_idx < (int)aggregations_.size();cell_idx++){


      const AggrOp aggregation = aggregations_[cell_idx];

      Value cell;
      AttrType attr_type = AttrType::INTS;

      switch(aggregation){
        case AggrOp::AGGR_MAX:
          rc = tuple->cell_at(cell_idx,cell);
          attr_type = cell.attr_type();
          if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
            /*answer+=cell.get_float();
            answer+=result_cells[0].get_float();
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());*/
            if(cell.get_float()>answer_min)
            {
              answer_min=cell.get_float();
              result_cells[cell_idx].set_float(answer_min);
            }
            else
            {
              result_cells[cell_idx].set_float(result_cells[cell_idx].get_float());
            }
          }
          if(attr_type == AttrType::CHARS){
            if(cell.get_string()>min_str)
            {
              min_str=cell.get_string();
              strcpy(ans_char,min_str.c_str());
              result_cells[cell_idx].set_string(ans_char);
            }
          }

          break;
        case AggrOp::AGGR_MIN:
          rc = tuple->cell_at(cell_idx,cell);
          attr_type = cell.attr_type();
          if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
            if(cell.get_float()<answer_max)
            {
              answer_max=cell.get_float();
              result_cells[cell_idx].set_float(answer_max);
            }
            else
            {
              result_cells[cell_idx].set_float(result_cells[cell_idx].get_float());
            }

          }

          if(attr_type == AttrType::CHARS){
            if(cell.get_string()<max_str)
            {
              max_str=cell.get_string();
              strcpy(ans_char,max_str.c_str());
              result_cells[cell_idx].set_string(ans_char);
            }
          }

          break;
        case AggrOp::AGGR_COUNT:
          rc = tuple->cell_at(cell_idx,cell);
          attr_type = cell.attr_type();
          if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
            index+=1;
            result_cells[cell_idx].set_float(index);
          }
          break;
        case AggrOp::AGGR_COUNT_ALL:
          rc = tuple->cell_at(cell_idx,cell);
          attr_type = cell.attr_type();
          if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
            index+=1;
            result_cells[cell_idx].set_float(index);
          }

          break;
        case AggrOp::AGGR_AVG:
          rc = tuple->cell_at(cell_idx,cell);
          attr_type = cell.attr_type();
          if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS){
            index+=1;
            sum+=cell.get_float();
            answer=sum/index;
            result_cells[cell_idx].set_float(answer);
            
          }

          break;
        default:
          return RC::UNIMPLENMENT;
      } 

    }

  }
  if(rc == RC::RECORD_EOF){
    rc=RC::SUCCESS;
  }

  result_tuple_.set_cells(result_cells);

  return rc;


}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
