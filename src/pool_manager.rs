use std::collections::VecDeque;

pub struct PoolManager {
    pub name: String,
    pub instances: VecDeque<Box<dyn FnOnce()>>,
    pub instance_constructor: Box<dyn FnMut() -> Box<dyn FnOnce()>>,
    pub metric_labels: Vec<(String, String)>,
}

impl PoolManager {
    pub fn set_desired_instances_absolute(&mut self, count: u32) {
        while u32::try_from(self.instances.len()).unwrap() < count {
            self.instances.push_back((self.instance_constructor)());
        }

        while u32::try_from(self.instances.len()).unwrap() > count {
            self.instances.pop_front().unwrap()();
        }
    }

    pub fn set_desired_instances_relative(&mut self, factor: f32) {
        self.set_desired_instances_absolute(
            u32::try_from((self.instances.len() as f32 * factor) as i64).unwrap(),
        )
    }

    pub fn set_desired_instances_delta(&mut self, delta: i32) {
        self.set_desired_instances_absolute(
            u32::try_from(i64::try_from(self.instances.len()).unwrap() + delta as i64).unwrap(),
        )
    }
}
