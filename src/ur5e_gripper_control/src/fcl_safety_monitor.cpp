#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <moveit/collision_detection/collision_common.h>
#include <moveit/planning_scene_monitor/planning_scene_monitor.h>
#include <moveit/robot_state/conversions.h>
#include <moveit_msgs/msg/display_trajectory.hpp>
#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>

using namespace std::chrono_literals;

class FclSafetyMonitorNode : public rclcpp::Node
{
public:
  explicit FclSafetyMonitorNode(const rclcpp::NodeOptions& options)
  : Node("fcl_safety_monitor", options)
  {
    arm_group_ = declare_parameter<std::string>("arm_group", "ur_manipulator");
    right_arm_group_ = declare_parameter<std::string>("right_arm_group", "right_ur_manipulator");
    master_arm_group_ = declare_parameter<std::string>("master_arm_group", arm_group_);
    monitor_trajectory_topic_ =
      declare_parameter<std::string>("monitor_trajectory_topic", "/fcl_monitor/display_trajectory");
    prediction_horizon_ = declare_parameter<double>("prediction_horizon", 0.5);
    prediction_sampling_step_ = declare_parameter<double>("prediction_sampling_step", 0.05);
    monitor_period_ = declare_parameter<double>("monitor_period", 0.1);
    required_state_max_age_ = declare_parameter<double>("required_state_max_age", 1.0);
    warn_distance_ = declare_parameter<double>("warn_distance", 0.12);
    stop_distance_ = declare_parameter<double>("stop_distance", 0.08);
    collision_distance_ = declare_parameter<double>("collision_distance", 0.0);
    distance_request_threshold_ = declare_parameter<double>("distance_request_threshold", 0.25);
    max_contacts_ = declare_parameter<int>("max_contacts", 4);
    console_log_every_n_cycles_ = declare_parameter<int>("console_log_every_n_cycles", 10);
    console_log_on_change_only_ = declare_parameter<bool>("console_log_on_change_only", true);
    monitor_risk_topic_ =
      declare_parameter<std::string>("monitor_risk_topic", "/fcl_monitor/risk_state");
    ignored_monitor_pairs_ = declare_parameter<std::vector<std::string>>(
      "ignored_monitor_pairs",
      {
        "base_link_inertia|table",
        "right_base_link_inertia|table",
        "base_link_inertia|upper_arm_link",
        "right_base_link_inertia|right_upper_arm_link",
      });
    for (const auto& pair : ignored_monitor_pairs_) {
      const auto separator = pair.find('|');
      if (separator == std::string::npos) {
        continue;
      }
      ignored_pair_keys_.insert(normalizePairKey(pair.substr(0, separator), pair.substr(separator + 1)));
    }
    monitor_log_file_ =
      expandPath(declare_parameter<std::string>("monitor_log_file", "/tmp/ur5e_fcl_monitor.csv"));

    planning_scene_service_ =
      declare_parameter<std::string>("planning_scene_service", "/get_planning_scene");
    planning_scene_topic_ = declare_parameter<std::string>("planning_scene_topic", "/planning_scene");
    planning_scene_world_topic_ =
      declare_parameter<std::string>("planning_scene_world_topic", "/planning_scene_world");
    collision_object_topic_ =
      declare_parameter<std::string>("collision_object_topic", "/collision_object");
    joint_states_topic_ = declare_parameter<std::string>("joint_states_topic", "/joint_states");
    attached_collision_object_topic_ = declare_parameter<std::string>(
      "attached_collision_object_topic", "/attached_collision_object");
    monitor_state_update_frequency_ =
      declare_parameter<double>("monitor_state_update_frequency", 30.0);
    load_octomap_monitor_ = declare_parameter<bool>("load_octomap_monitor", false);
  }

  bool init()
  {
    if (!prepareLogFile()) {
      return false;
    }

    planning_scene_monitor_ = std::make_shared<planning_scene_monitor::PlanningSceneMonitor>(
      shared_from_this(), "robot_description", "fcl_safety_monitor");
    if (!planning_scene_monitor_ || !planning_scene_monitor_->getRobotModel()) {
      RCLCPP_ERROR(get_logger(), "Failed to construct PlanningSceneMonitor.");
      return false;
    }

    planning_scene_monitor_->startStateMonitor(joint_states_topic_, attached_collision_object_topic_);
    planning_scene_monitor_->setStateUpdateFrequency(monitor_state_update_frequency_);
    planning_scene_monitor_->startSceneMonitor(planning_scene_topic_);
    planning_scene_monitor_->startWorldGeometryMonitor(
      collision_object_topic_, planning_scene_world_topic_, load_octomap_monitor_);

    if (!planning_scene_monitor_->requestPlanningSceneState(planning_scene_service_)) {
      RCLCPP_WARN(
        get_logger(), "Failed to request planning scene from %s. Monitor will rely on live topics.",
        planning_scene_service_.c_str());
    }

    buildLinkSets();

    trajectory_subscription_ = create_subscription<moveit_msgs::msg::DisplayTrajectory>(
      monitor_trajectory_topic_, rclcpp::SystemDefaultsQoS(),
      std::bind(&FclSafetyMonitorNode::handleTrajectory, this, std::placeholders::_1));
    risk_state_publisher_ =
      create_publisher<std_msgs::msg::String>(monitor_risk_topic_, rclcpp::SystemDefaultsQoS());

    timer_ = create_wall_timer(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::duration<double>(std::max(0.02, monitor_period_))),
      std::bind(&FclSafetyMonitorNode::runMonitorCycle, this));

    RCLCPP_INFO(
      get_logger(),
      "FCL monitor started. topic=%s risk_topic=%s horizon=%.3fs step=%.3fs master=%s log=%s",
      monitor_trajectory_topic_.c_str(), monitor_risk_topic_.c_str(), prediction_horizon_,
      prediction_sampling_step_, master_arm_group_.c_str(), monitor_log_file_.c_str());
    return true;
  }

private:
  struct MonitoredState
  {
    double distance{ std::numeric_limits<double>::infinity() };
    bool collision{ false };
    double penetration_depth{ 0.0 };
    double future_offset{ 0.0 };
    std::string first_body;
    std::string second_body;
    std::string pair_scope{ "unknown" };
  };

  static std::string normalizePairKey(const std::string& first, const std::string& second)
  {
    if (first <= second) {
      return first + "|" + second;
    }
    return second + "|" + first;
  }

  struct ActiveTrajectory
  {
    struct TimedWaypoint
    {
      double time_from_start{ 0.0 };
      moveit::core::RobotStatePtr state;
    };

    std::string group_name;
    std::vector<std::string> joint_names;
    std::vector<TimedWaypoint> waypoints;
    rclcpp::Time execution_start_time{ 0, 0, RCL_ROS_TIME };
    double duration{ 0.0 };
  };

  static std::string expandPath(const std::string& input)
  {
    if (!input.empty() && input.front() == '~') {
      const char* home = std::getenv("HOME");
      if (home != nullptr) {
        if (input.size() == 1) {
          return std::string(home);
        }
        if (input[1] == '/') {
          return std::string(home) + input.substr(1);
        }
      }
    }
    return input;
  }

  bool prepareLogFile()
  {
    try {
      const std::filesystem::path log_path(monitor_log_file_);
      if (!log_path.parent_path().empty()) {
        std::filesystem::create_directories(log_path.parent_path());
      }

      const bool already_exists = std::filesystem::exists(log_path);
      log_stream_.open(log_path, std::ios::out | std::ios::app);
      if (!log_stream_.is_open()) {
        RCLCPP_ERROR(get_logger(), "Failed to open monitor log file: %s", monitor_log_file_.c_str());
        return false;
      }

      log_stream_ << std::fixed << std::setprecision(6);
      if (!already_exists || std::filesystem::file_size(log_path) == 0U) {
        log_stream_
          << "stamp,current_distance,current_collision,current_penetration,current_body_1,current_body_2,"
          << "current_scope,predicted_distance,predicted_collision,predicted_penetration,predicted_body_1,"
          << "predicted_body_2,predicted_scope,predicted_tau,risk,master_arm,collision_detector\n";
        log_stream_.flush();
      }
      return true;
    } catch (const std::exception& ex) {
      RCLCPP_ERROR(get_logger(), "Failed to prepare monitor log file: %s", ex.what());
      return false;
    }
  }

  void buildLinkSets()
  {
    const auto robot_model = planning_scene_monitor_->getRobotModel();
    const auto* left_group = robot_model->getJointModelGroup(arm_group_);
    const auto* right_group = robot_model->getJointModelGroup(right_arm_group_);

    if (left_group != nullptr) {
      const auto& links = left_group->getLinkModelNamesWithCollisionGeometry();
      left_links_.insert(links.begin(), links.end());
    }
    if (right_group != nullptr) {
      const auto& links = right_group->getLinkModelNamesWithCollisionGeometry();
      right_links_.insert(links.begin(), links.end());
    }
  }

  void handleTrajectory(const moveit_msgs::msg::DisplayTrajectory::SharedPtr msg)
  {
    if (msg->trajectory.empty()) {
      return;
    }

    const auto& joint_trajectory = msg->trajectory.front().joint_trajectory;
    if (joint_trajectory.points.empty()) {
      RCLCPP_WARN(get_logger(), "Monitored trajectory has no points, ignore it.");
      return;
    }

    const auto scene = planning_scene_monitor_->copyPlanningScene();
    if (!scene) {
      RCLCPP_WARN(get_logger(), "Planning scene copy unavailable; ignore monitored trajectory.");
      return;
    }

    moveit::core::RobotState reference_state(scene->getRobotModel());
    bool start_state_loaded = moveit::core::robotStateMsgToRobotState(
      scene->getTransforms(), msg->trajectory_start, reference_state);
    if (!start_state_loaded) {
      reference_state = scene->getCurrentState();
      RCLCPP_WARN(
        get_logger(), "Failed to deserialize trajectory start state. Falling back to current scene state.");
    }

    ActiveTrajectory parsed_trajectory;
    parsed_trajectory.group_name = msg->model_id.empty() ? inferGroupName(joint_trajectory.joint_names) : msg->model_id;
    parsed_trajectory.joint_names = joint_trajectory.joint_names;
    parsed_trajectory.waypoints.reserve(joint_trajectory.points.size());

    for (const auto& point : joint_trajectory.points) {
      if (point.positions.size() != joint_trajectory.joint_names.size()) {
        RCLCPP_WARN(
          get_logger(),
          "Skip malformed monitored trajectory point: joint_names=%zu positions=%zu",
          joint_trajectory.joint_names.size(), point.positions.size());
        continue;
      }

      auto waypoint_state = std::make_shared<moveit::core::RobotState>(reference_state);
      waypoint_state->setVariablePositions(joint_trajectory.joint_names, point.positions);
      waypoint_state->update();
      parsed_trajectory.waypoints.push_back(ActiveTrajectory::TimedWaypoint{
        rclcpp::Duration(point.time_from_start).seconds(),
        waypoint_state,
      });
    }

    if (parsed_trajectory.waypoints.empty()) {
      RCLCPP_WARN(get_logger(), "All monitored trajectory points were invalid, ignore trajectory.");
      return;
    }

    rclcpp::Time execution_start_time = now();
    const auto& joint_header = joint_trajectory.header;
    if (joint_header.stamp.sec != 0 || joint_header.stamp.nanosec != 0) {
      execution_start_time = rclcpp::Time(joint_header.stamp, get_clock()->get_clock_type());
    }
    parsed_trajectory.execution_start_time = execution_start_time;
    parsed_trajectory.duration = parsed_trajectory.waypoints.back().time_from_start;

    {
      std::lock_guard<std::mutex> lock(trajectory_mutex_);
      active_trajectories_[parsed_trajectory.group_name] = parsed_trajectory;
    }

    RCLCPP_INFO(
      get_logger(), "Received monitored trajectory. group=%s duration=%.3fs waypoints=%zu",
      parsed_trajectory.group_name.c_str(), parsed_trajectory.duration, parsed_trajectory.waypoints.size());
  }

  void runMonitorCycle()
  {
    const auto scene = planning_scene_monitor_->copyPlanningScene();
    if (!scene) {
      return;
    }

    if (!hasFreshRobotState()) {
      return;
    }

    if (!collision_detector_logged_) {
      const std::string detector_name = scene->getCollisionDetectorName();
      collision_detector_name_ = detector_name.empty() ? "unknown" : detector_name;
      collision_detector_logged_ = true;
      RCLCPP_INFO(
        get_logger(), "Active collision detector: %s", collision_detector_name_.c_str());
      if (collision_detector_name_.find("FCL") == std::string::npos &&
          collision_detector_name_.find("fcl") == std::string::npos) {
        RCLCPP_WARN(
          get_logger(), "Current collision detector is not FCL: %s", collision_detector_name_.c_str());
      }
    }

    const auto stamp = now();
    const MonitoredState current_state = sanitizeMonitoredState(
      evaluateState(*scene, scene->getCurrentState(), 0.0));
    auto predicted_state = evaluatePredictedWindow(*scene, stamp);
    const MonitoredState* risk_source = &current_state;
    if (predicted_state.has_value() && isPreferredRiskCandidate(*predicted_state, *risk_source)) {
      risk_source = &*predicted_state;
    }
    const std::string risk = classifyRisk(*risk_source);

    writeLogLine(stamp, current_state, predicted_state, risk);
    publishRiskState(stamp, *risk_source, risk);

    ++cycle_count_;
    const bool periodic_slot =
      console_log_every_n_cycles_ > 0 && (cycle_count_ % console_log_every_n_cycles_ == 0);
    const bool summary_changed = hasConsoleSummaryChanged(*risk_source, risk);
    if ((periodic_slot && !console_log_on_change_only_) || summary_changed) {
      RCLCPP_INFO(
        get_logger(),
        "risk=%s current_d=%.4f predicted_d=%.4f pair=%s <-> %s tau=%.3fs scope=%s",
        risk.c_str(), current_state.distance,
        predicted_state.has_value() ? predicted_state->distance : current_state.distance,
        risk_source->first_body.c_str(), risk_source->second_body.c_str(), risk_source->future_offset,
        risk_source->pair_scope.c_str());
      rememberConsoleSummary(*risk_source, risk);
    }
  }

  std::optional<MonitoredState> evaluatePredictedWindow(
    const planning_scene::PlanningScene& scene, const rclcpp::Time& stamp)
  {
    std::map<std::string, ActiveTrajectory> active_trajectories;
    {
      std::lock_guard<std::mutex> lock(trajectory_mutex_);
      active_trajectories = active_trajectories_;
    }

    if (active_trajectories.empty()) {
      return std::nullopt;
    }

    std::optional<MonitoredState> best_state;
    std::vector<std::string> expired_groups;
    for (double future_offset = 0.0; future_offset <= prediction_horizon_ + 1e-9;
         future_offset += prediction_sampling_step_) {
      moveit::core::RobotState merged_state(scene.getCurrentState());
      bool has_active_prediction = false;

      for (const auto& [group_name, trajectory] : active_trajectories) {
        if (trajectory.waypoints.empty() || trajectory.duration <= 0.0) {
          expired_groups.push_back(group_name);
          continue;
        }

        const double elapsed = std::max(0.0, (stamp - trajectory.execution_start_time).seconds());
        if (elapsed > trajectory.duration + prediction_sampling_step_) {
          expired_groups.push_back(group_name);
          continue;
        }

        const double query_time = std::min(trajectory.duration, elapsed + future_offset);
        auto predicted_state = interpolateTrajectoryState(trajectory, query_time);
        if (!predicted_state) {
          continue;
        }

        applyTrajectoryState(trajectory, *predicted_state, merged_state);
        has_active_prediction = true;
      }

      if (!has_active_prediction) {
        continue;
      }

      merged_state.update();
      MonitoredState candidate = sanitizeMonitoredState(evaluateState(scene, merged_state, future_offset));
      if (!best_state.has_value() || isPreferredRiskCandidate(candidate, *best_state)) {
        best_state = candidate;
      }
      if (candidate.collision) {
        break;
      }
    }

    if (!expired_groups.empty()) {
      std::lock_guard<std::mutex> lock(trajectory_mutex_);
      for (const auto& group_name : expired_groups) {
        active_trajectories_.erase(group_name);
      }
    }

    return best_state;
  }

  moveit::core::RobotStatePtr interpolateTrajectoryState(
    const ActiveTrajectory& trajectory, double query_time) const
  {
    if (trajectory.waypoints.empty()) {
      return nullptr;
    }

    if (query_time <= trajectory.waypoints.front().time_from_start) {
      return std::make_shared<moveit::core::RobotState>(*trajectory.waypoints.front().state);
    }

    for (std::size_t index = 1; index < trajectory.waypoints.size(); ++index) {
      const auto& previous = trajectory.waypoints[index - 1];
      const auto& next = trajectory.waypoints[index];
      if (query_time > next.time_from_start) {
        continue;
      }

      if (next.time_from_start <= previous.time_from_start + 1e-9) {
        return std::make_shared<moveit::core::RobotState>(*next.state);
      }

      const double blend =
        (query_time - previous.time_from_start) / (next.time_from_start - previous.time_from_start);
      auto interpolated_state = std::make_shared<moveit::core::RobotState>(*previous.state);
      previous.state->interpolate(*next.state, blend, *interpolated_state);
      interpolated_state->update();
      return interpolated_state;
    }

    return std::make_shared<moveit::core::RobotState>(*trajectory.waypoints.back().state);
  }

  void applyTrajectoryState(
    const ActiveTrajectory& trajectory, const moveit::core::RobotState& source_state,
    moveit::core::RobotState& target_state) const
  {
    if (trajectory.joint_names.empty()) {
      return;
    }

    std::vector<double> joint_positions;
    joint_positions.reserve(trajectory.joint_names.size());
    for (const auto& joint_name : trajectory.joint_names) {
      joint_positions.push_back(source_state.getVariablePosition(joint_name));
    }
    target_state.setVariablePositions(trajectory.joint_names, joint_positions);
  }

  std::string inferGroupName(const std::vector<std::string>& joint_names) const
  {
    if (!joint_names.empty() && joint_names.front().rfind("right_", 0) == 0) {
      return right_arm_group_;
    }
    return arm_group_;
  }

  bool hasFreshRobotState()
  {
    const auto state_monitor = planning_scene_monitor_->getStateMonitor();
    if (!state_monitor) {
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), 5000, "CurrentStateMonitor is not ready yet.");
      return false;
    }

    const auto max_age = rclcpp::Duration::from_seconds(std::max(0.0, required_state_max_age_));
    if (!state_monitor->haveCompleteState(max_age)) {
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), 5000,
        "Waiting for complete fresh joint state before running FCL checks.");
      return false;
    }

    return true;
  }

  MonitoredState evaluateState(
    const planning_scene::PlanningScene& scene, const moveit::core::RobotState& state,
    double future_offset) const
  {
    MonitoredState result;
    result.future_offset = future_offset;

    const auto& acm = scene.getAllowedCollisionMatrix();
    const auto collision_env = scene.getCollisionEnv();

    collision_detection::DistanceRequest distance_request;
    distance_request.acm = &acm;
    distance_request.enable_nearest_points = true;
    distance_request.enable_signed_distance = true;
    distance_request.distance_threshold = distance_request_threshold_;

    collision_detection::DistanceResult self_result;
    collision_detection::DistanceResult world_result;
    collision_env->distanceSelf(distance_request, self_result, state);
    collision_env->distanceRobot(distance_request, world_result, state);

    const auto& best_result =
      (self_result.minimum_distance.distance <= world_result.minimum_distance.distance) ? self_result : world_result;
    result.distance = best_result.minimum_distance.distance;
    result.first_body = best_result.minimum_distance.link_names[0];
    result.second_body = best_result.minimum_distance.link_names[1];
    result.pair_scope = classifyPairScope(best_result.minimum_distance);

    collision_detection::CollisionRequest collision_request;
    collision_request.distance = true;
    collision_request.detailed_distance = true;
    collision_request.contacts = true;
    collision_request.max_contacts = static_cast<std::size_t>(std::max(1, max_contacts_));
    collision_request.max_contacts_per_pair = 1;

    collision_detection::CollisionResult collision_result;
    scene.checkCollision(collision_request, collision_result, state, acm);
    result.collision = collision_result.collision;

    for (const auto& contact_entry : collision_result.contacts) {
      for (const auto& contact : contact_entry.second) {
        result.penetration_depth = std::max(result.penetration_depth, contact.depth);
      }
    }

    if ((result.first_body.empty() || result.second_body.empty()) && !collision_result.contacts.empty()) {
      const auto& first_contact = *collision_result.contacts.begin();
      result.first_body = first_contact.first.first;
      result.second_body = first_contact.first.second;
      result.pair_scope = "collision_contact";
    }

    if (!std::isfinite(result.distance)) {
      result.distance = result.collision ? collision_distance_ : distance_request_threshold_;
    }

    return result;
  }

  std::string classifyPairScope(const collision_detection::DistanceResultsData& data) const
  {
    using collision_detection::BodyType;
    if (data.body_types[0] == BodyType::WORLD_OBJECT || data.body_types[1] == BodyType::WORLD_OBJECT) {
      return "world";
    }

    const std::string owner_1 = getArmOwner(data.link_names[0]);
    const std::string owner_2 = getArmOwner(data.link_names[1]);
    if (owner_1 == owner_2) {
      if (owner_1 == "left") {
        return "left_self";
      }
      if (owner_1 == "right") {
        return "right_self";
      }
      return "robot_internal";
    }
    return "inter_arm";
  }

  std::string getArmOwner(const std::string& link_name) const
  {
    if (left_links_.count(link_name) > 0U) {
      return "left";
    }
    if (right_links_.count(link_name) > 0U) {
      return "right";
    }
    if (link_name.rfind("right_", 0) == 0) {
      return "right";
    }
    if (!link_name.empty() && link_name != "world") {
      return "left";
    }
    return "unknown";
  }

  std::string classifyRisk(const MonitoredState& state) const
  {
    if (state.collision || state.penetration_depth > 0.0 || state.distance <= collision_distance_) {
      return "collision";
    }
    if (state.distance <= stop_distance_) {
      return "stop";
    }
    if (state.distance <= warn_distance_) {
      return "warn";
    }
    return "safe";
  }

  int riskSeverity(const std::string& risk) const
  {
    if (risk == "collision") {
      return 3;
    }
    if (risk == "stop") {
      return 2;
    }
    if (risk == "warn") {
      return 1;
    }
    return 0;
  }

  bool isPreferredRiskCandidate(const MonitoredState& candidate, const MonitoredState& incumbent) const
  {
    const std::string candidate_risk = classifyRisk(candidate);
    const std::string incumbent_risk = classifyRisk(incumbent);
    const bool candidate_inter_arm_actionable =
      candidate.pair_scope == "inter_arm" && riskSeverity(candidate_risk) > 0;
    const bool incumbent_inter_arm_actionable =
      incumbent.pair_scope == "inter_arm" && riskSeverity(incumbent_risk) > 0;

    if (candidate_inter_arm_actionable != incumbent_inter_arm_actionable) {
      return candidate_inter_arm_actionable;
    }

    const int candidate_severity = riskSeverity(candidate_risk);
    const int incumbent_severity = riskSeverity(incumbent_risk);
    if (candidate_severity != incumbent_severity) {
      return candidate_severity > incumbent_severity;
    }

    if (std::abs(candidate.distance - incumbent.distance) > 1e-6) {
      return candidate.distance < incumbent.distance;
    }

    return candidate.future_offset < incumbent.future_offset;
  }

  MonitoredState sanitizeMonitoredState(const MonitoredState& state) const
  {
    if (!shouldIgnorePair(state.first_body, state.second_body)) {
      return state;
    }

    MonitoredState sanitized = state;
    sanitized.distance = distance_request_threshold_;
    sanitized.collision = false;
    sanitized.penetration_depth = 0.0;
    sanitized.pair_scope = "ignored_noise";
    return sanitized;
  }

  bool shouldIgnorePair(const std::string& first, const std::string& second) const
  {
    if (first.empty() || second.empty()) {
      return false;
    }
    return ignored_pair_keys_.count(normalizePairKey(first, second)) > 0U;
  }

  std::string recommendedActionForRisk(const std::string& risk) const
  {
    if (risk == "warn") {
      return "slowdown";
    }
    if (risk == "stop") {
      return "stop";
    }
    if (risk == "collision") {
      return "replan";
    }
    return "none";
  }

  void publishRiskState(const rclcpp::Time& stamp, const MonitoredState& state, const std::string& risk)
  {
    if (!risk_state_publisher_) {
      return;
    }

    std_msgs::msg::String msg;
    msg.data = risk + "|" + recommendedActionForRisk(risk) + "|" + state.pair_scope + "|" + state.first_body +
               "|" + state.second_body + "|" + std::to_string(state.future_offset) + "|" +
               std::to_string(stamp.seconds());
    risk_state_publisher_->publish(msg);
  }

  bool hasConsoleSummaryChanged(const MonitoredState& state, const std::string& risk) const
  {
    return risk != last_console_risk_ || state.pair_scope != last_console_pair_scope_ ||
           state.first_body != last_console_first_body_ || state.second_body != last_console_second_body_;
  }

  void rememberConsoleSummary(const MonitoredState& state, const std::string& risk)
  {
    last_console_risk_ = risk;
    last_console_pair_scope_ = state.pair_scope;
    last_console_first_body_ = state.first_body;
    last_console_second_body_ = state.second_body;
  }

  void writeLogLine(
    const rclcpp::Time& stamp, const MonitoredState& current_state,
    const std::optional<MonitoredState>& predicted_state, const std::string& risk)
  {
    if (!log_stream_.is_open()) {
      return;
    }

    log_stream_
      << stamp.seconds() << ','
      << current_state.distance << ','
      << static_cast<int>(current_state.collision) << ','
      << current_state.penetration_depth << ','
      << current_state.first_body << ','
      << current_state.second_body << ','
      << current_state.pair_scope << ',';

    if (predicted_state.has_value()) {
      log_stream_
        << predicted_state->distance << ','
        << static_cast<int>(predicted_state->collision) << ','
        << predicted_state->penetration_depth << ','
        << predicted_state->first_body << ','
        << predicted_state->second_body << ','
        << predicted_state->pair_scope << ','
        << predicted_state->future_offset << ',';
    } else {
      log_stream_ << ",,,,,,,";
    }

    log_stream_
      << risk << ','
      << master_arm_group_ << ','
      << collision_detector_name_ << '\n';
    log_stream_.flush();
  }

  planning_scene_monitor::PlanningSceneMonitorPtr planning_scene_monitor_;
  rclcpp::Subscription<moveit_msgs::msg::DisplayTrajectory>::SharedPtr trajectory_subscription_;
  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr risk_state_publisher_;
  rclcpp::TimerBase::SharedPtr timer_;

  std::mutex trajectory_mutex_;
  std::map<std::string, ActiveTrajectory> active_trajectories_;

  std::ofstream log_stream_;
  std::set<std::string> left_links_;
  std::set<std::string> right_links_;
  std::set<std::string> ignored_pair_keys_;

  std::string arm_group_;
  std::string right_arm_group_;
  std::string master_arm_group_;
  std::string monitor_trajectory_topic_;
  std::string monitor_risk_topic_;
  std::string monitor_log_file_;
  std::string planning_scene_service_;
  std::string planning_scene_topic_;
  std::string planning_scene_world_topic_;
  std::string collision_object_topic_;
  std::string joint_states_topic_;
  std::string attached_collision_object_topic_;
  std::string collision_detector_name_{ "unknown" };

  double prediction_horizon_{};
  double prediction_sampling_step_{};
  double monitor_period_{};
  double required_state_max_age_{};
  double warn_distance_{};
  double stop_distance_{};
  double collision_distance_{};
  double distance_request_threshold_{};
  double monitor_state_update_frequency_{};

  int max_contacts_{};
  int console_log_every_n_cycles_{};
  std::size_t cycle_count_{ 0U };
  bool console_log_on_change_only_{};

  std::vector<std::string> ignored_monitor_pairs_;
  std::string last_console_risk_{ "uninitialized" };
  std::string last_console_pair_scope_;
  std::string last_console_first_body_;
  std::string last_console_second_body_;

  bool load_octomap_monitor_{};
  bool collision_detector_logged_{ false };
};

int main(int argc, char** argv)
{
  rclcpp::init(argc, argv);

  auto node = std::make_shared<FclSafetyMonitorNode>(rclcpp::NodeOptions{});
  if (!node->init()) {
    rclcpp::shutdown();
    return 1;
  }

  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
