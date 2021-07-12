#pragma once

#include <map>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/action/generators/abstract_action_generator.h"

namespace noisepage {

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate create/drop index candidate actions
 */
class NoOpActionGenerator : AbstractActionGenerator {
 public:
  void GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                       common::ManagedPointer<settings::SettingsManager> settings_manager,
                       std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                       std::vector<action_id_t> *candidate_actions) override;
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
