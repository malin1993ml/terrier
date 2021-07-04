#include "self_driving/planning/action/generators/no_op_action_generator.h"

#include "self_driving/planning/action/no_op_action.h"

namespace noisepage::selfdriving::pilot {

void NoOpActionGenerator::GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                                                common::ManagedPointer<settings::SettingsManager> settings_manager,
                                                std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                                std::vector<action_id_t> *candidate_actions) {
  // Add the no-op action
  auto no_op_action = std::make_unique<NoOpAction>();
  action_id_t action_id = no_op_action->GetActionID();
  action_map->emplace(action_id, std::move(no_op_action));
  candidate_actions->emplace_back(action_id);
  // Reverse action is itself
  action_map->at(action_id)->AddReverseAction(action_id);
}

}  // namespace noisepage::selfdriving::pilot
